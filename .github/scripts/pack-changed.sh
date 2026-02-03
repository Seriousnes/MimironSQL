#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required (GitHub Actions step output file)}"
: "${BASE_SHA:?BASE_SHA is required}"
: "${HEAD_SHA:?HEAD_SHA is required}"
: "${PACKAGE_VERSION:?PACKAGE_VERSION is required}"

repo_root="$(pwd)"

changed_files_path="$repo_root/changed_files.txt"
git diff --name-only "$BASE_SHA" "$HEAD_SHA" > "$changed_files_path" || true

mapfile -t csprojs < <(git ls-files '*.csproj')

declare -A packable
for project in "${csprojs[@]}"; do
  if grep -q '<PackageId>' "$project" && ! grep -q '<IsPackable>false</IsPackable>' "$project"; then
    packable["$project"]=1
  fi
done

declare -A referenced_by
for project in "${csprojs[@]}"; do
  project_dir="$(dirname "$project")"

  while IFS= read -r include; do
    [[ -z "$include" ]] && continue

    include="${include//\\//}"
    referenced_project="$(realpath --relative-to="$repo_root" "$project_dir/$include")"
    referenced_by["$referenced_project"]+="$project"$'\n'
  done < <(sed -n 's/.*<ProjectReference Include="\([^"]*\)".*/\1/p' "$project")
done

declare -A changed_projects
for project in "${csprojs[@]}"; do
  project_dir="$(dirname "$project")"
  if grep -q "^$project_dir/" "$changed_files_path"; then
    changed_projects["$project"]=1
  fi
done

declare -A visited
queue=()

for project in "${!changed_projects[@]}"; do
  visited["$project"]=1
  queue+=("$project")
done

while [[ ${#queue[@]} -ne 0 ]]; do
  project="${queue[0]}"
  queue=("${queue[@]:1}")

  while IFS= read -r dependent; do
    [[ -z "$dependent" ]] && continue
    if [[ -z "${visited[$dependent]:-}" ]]; then
      visited["$dependent"]=1
      queue+=("$dependent")
    fi
  done < <(printf '%s' "${referenced_by[$project]:-}")
done

declare -A selected
for project in "${!visited[@]}"; do
  if [[ -n "${packable[$project]:-}" ]]; then
    selected["$project"]=1
  fi
done

if [[ ${#selected[@]} -eq 0 ]]; then
  echo "any=false" >> "$GITHUB_OUTPUT"
  echo "projects=" >> "$GITHUB_OUTPUT"
  exit 0
fi

echo "any=true" >> "$GITHUB_OUTPUT"
{
  echo "projects<<EOF"
  for project in "${!selected[@]}"; do
    echo "$project"
  done | sort
  echo "EOF"
} >> "$GITHUB_OUTPUT"

mkdir -p ./packages

while IFS= read -r project; do
  [[ -z "$project" ]] && continue
  dotnet pack "$project" --configuration Release --no-build --output ./packages -p:Version="$PACKAGE_VERSION"
done < <(
  for project in "${!selected[@]}"; do
    echo "$project"
  done | sort
)
