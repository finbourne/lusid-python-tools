black .
if [ -n "$(git status --porcelain)" ]; then 
git commit -m "Travis making black code formatting" -a 
#git push https://\finbourne-bot-public:${GH_TOKEN}@github.com/\finbourne/lusid-python-tools.git HEAD:${GH_BRANCH}
git push origin ${GH_BRANCH}
echo "The black formating tool has made changes. Making commit."
else 
echo "No changes detected. Skipping commit"; 
fi
