#!/bin/bash -e

black .
if [ -n "$(git status --porcelain)" ]; then 
git commit -m "GitHub Actions making black code formatting" -a 
git push origin HEAD:${GH_BRANCH}
echo "The black formating tool has made changes. Making commit."
else 
echo "No changes detected. Skipping commit"; 
fi
