import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.identity import ClientSecretCredential
from azure.identity.aio import ClientSecretCredential as ClientSecretCredentialAsync
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError
from msgraph.core import GraphClient
import asyncio
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(filename='datalake_acls.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Obtener credenciales del Service Principal desde variables de entorno
client_id = os.environ.get('AZURE_CLIENT_ID')
client_secret = os.environ.get('AZURE_CLIENT_SECRET')
tenant_id = os.environ.get('AZURE_TENANT_ID')

if not all([client_id, client_secret, tenant_id]):
    logging.error("Missing required environment variables: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID")
    raise ValueError("Missing required environment variables for Service Principal authentication")

def get_azure_ad_group_object_id(group_name, graph_client):
    """Retrieve Azure AD group object ID by display name using Microsoft Graph."""
    try:
        result = graph_client.groups.get().filter(f"displayName eq '{group_name}'").select('id').get()
        groups = result.value
        if groups and len(groups) > 0:
            return groups[0].id
        logging.error(f"Group {group_name} not found in Azure AD.")
        return None
    except Exception as e:
        logging.error(f"Error retrieving group {group_name}: {str(e)}")
        return None

def get_environment_group(environment, graph_client):
    """Map environment to corresponding Azure AD group object ID."""
    env_groups = {
        'dev': 'grupo_dev',
        'pre': 'grupo_pre',
        'pro': 'grupo_pro'
    }
    group_name = env_groups.get(environment.lower(), 'grupo_dev')
    return get_azure_ad_group_object_id(group_name, graph_client)

def parse_permission(permission):
    """Convert permission shorthand to ACL format."""
    permissions = {
        'R': 'r-x',
        'W': 'rwx'
    }
    return permissions.get(permission.upper(), 'r-x')

def parse_acls(acl_string):
    """Parse ACL string into a dictionary, handling empty or malformed inputs."""
    acl_dict = {}
    if not acl_string:
        return acl_dict
    for entry in acl_string.split(','):
        if entry.startswith('group:') and len(entry.split(':')) >= 4:
            parts = entry.split(':')
            group_id = parts[1]
            permissions = parts[3]
            acl_dict[group_id] = permissions
    return acl_dict

def backup_acls_to_csv(storage_account_name, container_name, paths, file_system_client, backup_file):
    """Backup current ACLs for the specified paths to a CSV file."""
    with open(backup_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header if file is empty
        if csvfile.tell() == 0:
            writer.writerow(['storage', 'container', 'path', 'group_id', 'permissions'])
        
        for path in paths:
            try:
                directory_client = file_system_client.get_directory_client(path)
                acl_props = directory_client.get_access_control()
                current_acls = acl_props.get('acl', '')
                for acl in current_acls.split(',') if current_acls else []:
                    if acl.startswith('group:') and len(acl.split(':')) >= 4:
                        parts = acl.split(':')
                        group_id = parts[1]
                        permissions = parts[3]
                        writer.writerow([storage_account_name, container_name, path or 'root', group_id, permissions])
            except ResourceNotFoundError:
                logging.info(f"Directory {storage_account_name}/{container_name}/{path or 'root'} does not exist, skipping backup")
            except Exception as e:
                logging.error(f"Error backing up ACLs for {storage_account_name}/{container_name}/{path or 'root'}: {str(e)}")

async def apply_acls_to_path(row, service_clients, graph_client, backup_file):
    """Apply ACLs to the specified path and its parent directories in Azure Data Lake, with backup."""
    environment = row['environment']
    storage_account_name = row['storage']
    path = row['path']
    group_name = row['group_name']
    permission = row['permission']
    force_update = row['force_update'].lower() == 'true'
    force_propagation = row['force_propagation'].lower() == 'true'

    # Initialize DataLakeServiceClient if not already initialized
    if storage_account_name not in service_clients:
        try:
            credential = ClientSecretCredential(tenant_id, client_id, client_secret)
            service_clients[storage_account_name] = DataLakeServiceClient(
                account_url=f"https://{storage_account_name}.dfs.core.windows.net",
                credential=credential
            )
        except Exception as e:
            logging.error(f"Failed to initialize DataLakeServiceClient for {storage_account_name}: {str(e)}")
            return

    # Convert group_name to object ID
    group_object_id = get_azure_ad_group_object_id(group_name, graph_client)
    if not group_object_id:
        logging.error(f"Skipping {path}: Invalid group object ID for group_name={group_name}")
        return

    # Extract container and relative path
    path_parts = path.strip('/').split('/')
    container_name = path_parts[0] if path_parts else 'data'
    relative_path = '/'.join(path_parts[1:]) if len(path_parts) > 1 else ''  # Path without container
    acl_permission = parse_permission(permission)
    env_group_object_id = get_environment_group(environment, graph_client)

    if not env_group_object_id:
        logging.error(f"Skipping {path}: Invalid group object ID for environment={environment}")
        return

    try:
        # Get the file system client
        file_system_client = service_clients[storage_account_name].get_file_system_client(container_name)

        # Define paths to backup (target path and parent paths)
        parent_paths = []
        if len(path_parts) > 2:  # Include all parents from first subfolder to second-to-last
            parent_paths = ['/'.join(path_parts[1:i+1]) for i in range(1, len(path_parts) - 1)]
        elif len(path_parts) == 2:  # Path like container1/subfichero8 has no parent for --x
            parent_paths = []
        paths_to_backup = [relative_path] + parent_paths if relative_path else parent_paths

        # Backup current ACLs before making changes
        if paths_to_backup:
            backup_acls_to_csv(storage_account_name, container_name, paths_to_backup, file_system_client, backup_file)
            logging.info(f"Backed up ACLs for {storage_account_name}/{container_name}/{relative_path or 'root'} and parents to {backup_file}")

        # Get the directory client for the target path (last folder, e.g., c)
        directory_client = file_system_client.get_directory_client(relative_path)

        # Get current ACLs for the target directory
        try:
            acl_props = directory_client.get_access_control()
            current_acls = acl_props.get('acl', '')
        except ResourceNotFoundError:
            # Create directory if it doesn't exist
            directory_client.create_directory()
            current_acls = ''
            logging.info(f"Created directory {storage_account_name}/{container_name}/{relative_path or 'root'}")

        acl_dict = parse_acls(current_acls)
        current_perm = acl_dict.get(group_object_id, '')
        should_update = force_update or not current_perm or (permission == 'W' and current_perm != 'rwx')

        if should_update:
            new_acl = f"group:{group_object_id}:{acl_permission}"
            updated_acls = new_acl
            if current_acls:
                # Preserve other existing ACLs, update or add the new one
                existing_acls = [acl for acl in current_acls.split(',') if not acl.startswith(f"group:{group_object_id}:")]
                updated_acls = ','.join(existing_acls + [new_acl]) if existing_acls else new_acl
            directory_client.set_access_control_recursive(
                acl=updated_acls,
                mode='set' if force_propagation else 'modify'
            )
            logging.info(f"Set ACLs on {storage_account_name}/{container_name}/{relative_path or 'root'}: {updated_acls}, recursive={force_propagation}")
        else:
            logging.info(f"Skipping ACL update on {storage_account_name}/{container_name}/{relative_path or 'root'}: Existing permission ({current_perm}) is higher or equal and force_update=False")

        # Apply execution permissions (--x) to all parent directories except the last folder
        for parent_path in parent_paths:
            parent_directory_client = file_system_client.get_directory_client(parent_path)
            try:
                parent_acl_props = parent_directory_client.get_access_control()
                parent_acls = parent_acl_props.get('acl', '')
            except ResourceNotFoundError:
                # Create parent directory if it doesn't exist
                parent_directory_client.create_directory()
                parent_acls = ''
                logging.info(f"Created parent directory {storage_account_name}/{container_name}/{parent_path}")

            parent_acl_dict = parse_acls(parent_acls)
            if env_group_object_id not in parent_acl_dict or force_update:
                env_acl = f"group:{env_group_object_id}:--x"
                updated_parent_acls = env_acl
                if parent_acls:
                    # Preserve other existing ACLs, update or add the new one
                    existing_parent_acls = [acl for acl in parent_acls.split(',') if not acl.startswith(f"group:{env_group_object_id}:")]
                    updated_parent_acls = ','.join(existing_parent_acls + [env_acl]) if existing_parent_acls else env_acl
                parent_directory_client.set_access_control_recursive(
                    acl=updated_parent_acls,
                    mode='set'
                )
                logging.info(f"Set parent ACLs on {storage_account_name}/{container_name}/{parent_path}: {updated_parent_acls}")
            else:
                logging.info(f"Skipping parent ACL update on {storage_account_name}/{container_name}/{parent_path}: Existing permission ({parent_acl_dict.get(env_group_object_id)}) present and force_update=False")

        logging.info(f"Successfully processed ACLs for {path} in {storage_account_name}/{container_name}")
        
    except Exception as e:
        logging.error(f"Error processing ACLs for {path}: {str(e)}")

async def process_csv(csv_file_path, max_workers=4):
    """Process CSV file and apply ACLs to Azure Data Lake using multithreading."""
    # Initialize GraphClient with async credential
    credential = ClientSecretCredentialAsync(tenant_id, client_id, client_secret)
    graph_client = GraphClient(credential=credential)

    # Dictionary to store DataLakeServiceClient instances
    service_clients = {}
    
    # Generate unique backup file name based on current timestamp
    backup_file = f"backup_acls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    logging.info(f"Creating ACL backup file: {backup_file}")
    
    # Read all rows from CSV
    rows = []
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        required_columns = ['environment', 'storage', 'path', 'group_name', 'permission', 'force_update', 'force_propagation']
        
        for row in reader:
            if not all(col in row for col in required_columns):
                logging.error(f"Skipping row, missing required columns: {row}")
                continue
            rows.append(row)

    # Process rows using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.create_task(apply_acls_to_path(row, service_clients, graph_client, backup_file)) for row in rows]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python manage_datalake_acls.py <csv_file_path>")
        sys.exit(1)
    
    csv_file_path = sys.argv[1]
    if not os.path.exists(csv_file_path):
        print(f"CSV file not found: {csv_file_path}")
        sys.exit(1)
    
    # Run the async process_csv function
    asyncio.run(process_csv(csv_file_path))