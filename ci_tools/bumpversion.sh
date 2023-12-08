#!/bin/bash -e

last_commit_message=$(git log -1 HEAD --pretty=format:%s)

# Bump the version up

if [[ "$last_commit_message" == "Bump version"* ]]; then
	echo "Skipping version increment (no commits found)"
else
    # Bump the version in project and make commit back to master
	poetry version prerelease
	git add pyproject.toml
	git commit -m "Bump version to $(poetry version -s)"
	git push -f https://finbourne-bot-public:${GH_TOKEN}@github.com/finbourne/lusid-python-tools.git HEAD:master
	echo "Commit with incremented version completed"
fi
