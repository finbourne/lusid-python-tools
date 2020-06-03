black .
if [ -n "$(git status --porcelain)" ]; then 
git config --local user.email "engineering@finbourne.com" 
git config --local user.name "finbourne-bot-public" 
git commit -m "Travis making black code formatting" -a 
git push https://\finbourne-bot-public:${gh_token}@github.com/\finbourne/lusid-python-tools.git HEAD:$TRAVIS_BRANCH
echo "The black formating tool has made changes. Making commit."
else 
echo "No changes detected. Skipping commit"; 
fi