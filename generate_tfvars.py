import json
import os
import sys

# Variables de entorno (pasadas desde GitHub Actions)
repo_full = os.environ.get('REPO_NAME', 'sdsd-repo')  # github.repository
entity_prefix = repo_full.split('/')[-1][:3]  # 'sds'
env = os.environ.get('ENVIRONMENT', 'd1')
location = os.environ.get('LOCATION', 'westeurope')
fixed_suffix = 'tdpplatcrit001'

# Cargar mappings
try:
    with open('mappings.json', 'r') as f:
        mappings = json.load(f)
except FileNotFoundError:
    print("Error: mappings.json no encontrado")
    sys.exit(1)

location_code = mappings['locations'].get(location.lower(), 'unknown')
if location_code == 'unknown':
    print(f"Error: Location '{location}' no encontrado en mappings")
    sys.exit(1)

# Lista de componentes a generar (puedes pasar como env var o hardcodear)
components = ['log_analytics', 'eventhub', 'key_vault']

for component in components:
    component_code = mappings['components'].get(component, 'unknown')
    if component_code == 'unknown':
        print(f"Error: Componente '{component}' no encontrado")
        continue

    resource_name = f"{entity_prefix}{env}{location_code}{component_code}{fixed_suffix}"

    template_path = f"{component}.tfvars.template"
    output_path = f"generated_{component}.tfvars"

    if not os.path.exists(template_path):
        print(f"Template no encontrada: {template_path}")
        continue

    with open(template_path, 'r') as template_file:
        content = template_file.read()

    # Reemplazos simples
    content = content.replace('{{resource_name}}', resource_name)
    content = content.replace('{{location}}', location)
    content = content.replace('{{environment}}', env)

    with open(output_path, 'w') as output_file:
        output_file.write(content)

    print(f"Generado: {output_path} con nombre: {resource_name}")