#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required (GitHub Actions step output file)}"
: "${PACKAGE_VERSION:?PACKAGE_VERSION is required}"

BASE_SHA="${BASE_SHA:-}"
HEAD_SHA="${HEAD_SHA:-}"
FORCE_PACKAGE_IDS="${FORCE_PACKAGE_IDS:-}"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

git_commit_exists() {
  git cat-file -e "${1}^{commit}" 2>/dev/null
}

ensure_origin_main_ref() {
  if git show-ref --verify --quiet refs/remotes/origin/main; then
    return 0
  fi

  git fetch --no-tags --prune origin "+refs/heads/main:refs/remotes/origin/main" >/dev/null 2>&1 || true
}

try_fetch_commit() {
  git fetch --no-tags --prune --depth=1 origin "$1" >/dev/null 2>&1 || true
}

pack_all=false
manual_select=false

if [[ -n "$FORCE_PACKAGE_IDS" ]]; then
  manual_select=true
fi

base_sha="$BASE_SHA"
head_sha="$HEAD_SHA"

if [[ "$manual_select" != true ]]; then
  : "${BASE_SHA:?BASE_SHA is required unless FORCE_PACKAGE_IDS is set}"
  : "${HEAD_SHA:?HEAD_SHA is required unless FORCE_PACKAGE_IDS is set}"

  if ! git_commit_exists "$head_sha"; then
    echo "HEAD_SHA '$head_sha' is not a valid commit in this checkout." >&2
    exit 1
  fi

  if [[ "$base_sha" == "0000000000000000000000000000000000000000" ]]; then
    base_sha=""
  fi

  if [[ -n "$base_sha" ]] && ! git_commit_exists "$base_sha"; then
    try_fetch_commit "$base_sha"
  fi

  if [[ -z "$base_sha" ]] || ! git_commit_exists "$base_sha"; then
    ensure_origin_main_ref

    base_sha="$(git merge-base "$head_sha" "origin/main" 2>/dev/null || true)"
    if [[ -z "$base_sha" ]] || [[ "$base_sha" == "$head_sha" ]]; then
      base_sha="$(git rev-parse "$head_sha^" 2>/dev/null || true)"
    fi

    if [[ -z "$base_sha" ]] || ! git_commit_exists "$base_sha"; then
      echo "BASE_SHA '$BASE_SHA' is not available (force-push/history rewrite?)." >&2
      echo "Set FORCE_PACKAGE_IDS to force packing specific packages." >&2
      exit 1
    fi
  fi
fi

changed_files_path="$repo_root/changed_files.txt"

mapfile -t csprojs < <(git ls-files '*.csproj')

declare -A packable
declare -A project_for_package_id
for project in "${csprojs[@]}"; do
  if grep -q '<PackageId>' "$project" && ! grep -q '<IsPackable>false</IsPackable>' "$project"; then
    packable["$project"]=1

    package_id="$(sed -n 's:.*<PackageId>[[:space:]]*\([^<]*\)[[:space:]]*</PackageId>.*:\1:p' "$project" | head -n 1 | tr -d '\r')"
    if [[ -n "$package_id" ]]; then
      project_for_package_id["$package_id"]="$project"
    fi
  fi
done

declare -A selected=()

if [[ ${#packable[@]} -eq 0 ]]; then
  echo "any=false" >> "$GITHUB_OUTPUT"
  echo "projects=" >> "$GITHUB_OUTPUT"
  exit 0
fi

if [[ "$pack_all" == true ]]; then
  for project in "${!packable[@]}"; do
    selected["$project"]=1
  done
fi

if [[ "$manual_select" == true ]]; then
  while IFS= read -r package_id; do
    package_id="${package_id//$'\r'/}"
    [[ -z "$package_id" ]] && continue

    if [[ "$package_id" == "ALL" ]]; then
      pack_all=true
      continue
    fi

    project="${project_for_package_id[$package_id]:-}"
    if [[ -z "$project" ]]; then
      echo "Unknown PackageId '$package_id'." >&2
      echo "Known packable PackageIds:" >&2
      for known in "${!project_for_package_id[@]}"; do
        echo "- $known" >&2
      done | sort >&2
      exit 1
    fi

    selected["$project"]=1
  done < <(printf '%s\n' "$FORCE_PACKAGE_IDS")

  if [[ "$pack_all" == true ]]; then
    for project in "${!packable[@]}"; do
      selected["$project"]=1
    done
  fi
fi

declare -A referenced_by
declare -A references
for project in "${csprojs[@]}"; do
  project_dir="$(dirname "$project")"

  while IFS= read -r include; do
    [[ -z "$include" ]] && continue

    include="${include//\\//}"
    referenced_project="$(realpath --relative-to="$repo_root" "$project_dir/$include")"
    referenced_by["$referenced_project"]+="$project"$'\n'
    references["$project"]+="$referenced_project"$'\n'
  done < <(sed -n 's/.*<ProjectReference Include="\([^"]*\)".*/\1/p' "$project")
done

if [[ "$manual_select" != true ]]; then
  git diff --name-only "$base_sha" "$head_sha" > "$changed_files_path"

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

  for project in "${!visited[@]}"; do
    if [[ -n "${packable[$project]:-}" ]]; then
      selected["$project"]=1
    fi
  done
fi

queue=()
declare -A selected_visited
for project in "${!selected[@]}"; do
  selected_visited["$project"]=1
  queue+=("$project")
done

while [[ ${#queue[@]} -ne 0 ]]; do
  project="${queue[0]}"
  queue=("${queue[@]:1}")

  while IFS= read -r referenced; do
    [[ -z "$referenced" ]] && continue

    if [[ -z "${packable[$referenced]:-}" ]]; then
      continue
    fi

    if [[ -z "${selected_visited[$referenced]:-}" ]]; then
      selected_visited["$referenced"]=1
      selected["$referenced"]=1
      queue+=("$referenced")
    fi
  done < <(printf '%s' "${references[$project]:-}")
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
