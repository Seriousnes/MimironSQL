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

declare -A selected
for project in "${!packable[@]}"; do
  project_dir="$(dirname "$project")"
  if grep -q "^$project_dir/" "$changed_files_path"; then
    selected["$project"]=1
  fi
done

changed=1
while [[ $changed -eq 1 ]]; do
  changed=0
  for project in "${!packable[@]}"; do
    [[ -n "${selected[$project]:-}" ]] && continue

    project_dir="$(dirname "$project")"

    while IFS= read -r include; do
      [[ -z "$include" ]] && continue

      include="${include//\\//}"
      referenced_project="$(realpath --relative-to="$repo_root" "$project_dir/$include")"

      if [[ -n "${selected[$referenced_project]:-}" ]]; then
        selected["$project"]=1
        changed=1
        break
      fi
    done < <(sed -n 's/.*<ProjectReference Include="\([^"]*\)".*/\1/p' "$project")
  done
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
