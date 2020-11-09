#!/bin/bash

last_commit_message=$(git log -1 HEAD --pretty=format:%s)

# Bump the version up

if [[ "$last_commit_message" == "Bump version"* ]]; then
        echo "Skipping version increment (no commits found)"
else

	# Find next version

	api_version=$(cat __version__.py | grep __version__ |  awk '{split($0, a, "="); print a[2]}' | tr -d ' "')
	IFS='.' read -a version_split <<< "${api_version}"
	current_patch=${version_split[2]}
	new_patch=$(echo "$current_patch + 1" | tr -d $'\r' | bc)
	new_version="${version_split[0]}.${version_split[1]}.$new_patch"
    echo "Bumping packaging patch version from $api_version to $new_version"

    # Bump the version in project and make commit back to master

    bump2version patch --message "Bump version in Travis CI: {current_version} to {new_version} [skip ci]"
	git push https://finbourne-bot-public:${gh_token}@github.com/finbourne/lusid-python-tools.git HEAD:master
	echo "Commit with incremented version completed"
fi
