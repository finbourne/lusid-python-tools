black --check lusidtools;
if [ $? = 1 ]; then 
exit -1
echo "Error code is 1"
else 
exit 0
echo "Error code is 0";
fi