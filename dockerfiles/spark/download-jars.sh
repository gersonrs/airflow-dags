#!/bin/bash
set -e

# Caminho para o arquivo de dependências
DEPENDENCIES_FILE="dependencies.txt"

# Diretório onde os JARs serão salvos
DESTINATION_DIR="$SPARK_HOME/jars"

# Itera pelas dependências no arquivo
while IFS= read -r dependency; do
  IFS=':' read -r group module version <<< "$dependency"
  echo "Baixando: $group:$module:$version"

  # Use o Ivy para baixar a dependência
  java -jar "$SPARK_HOME/jars/ivy-2.5.0.jar" -dependency "$group" "$module" "$version" \
    -retrieve "$DESTINATION_DIR/[artifact]-[revision](-[classifier]).[ext]" -types "jar"
done < "$DEPENDENCIES_FILE"
