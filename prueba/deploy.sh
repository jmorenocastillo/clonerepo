echo "Ejecutando script desde repo externo"
echo "Variable VERSION: $VAR1"
echo "Variable ENVIRONMENT: $VAR2"
# repo-scripts/scripts/mi-script.sh
#!/bin/bash

# Leer las variables pasadas como argumentos o desde entorno
MAIN_VAR1=${1:-$VAR1}
MAIN_VAR2=${2:-$VAR2}

echo "Ejecutando script desde otro repo"
echo "Variable 1 recibida: $VAR1"
echo "Variable 2 recibida: $VAR2"

