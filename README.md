# Group29DDS
**In folder Group1, please provide a README.txt file to let me know anything you want the grader know. Keep the readme file short and concise.


Also, in your README file, in the first sentence, please let me know how you define your rectangle.. Some student use (topleft,bottomright), some use (bottomleft, topright). Please let me know how you define your rectangle.

In folder Group1, please create a folder (You can use any folder name, or use the github template) and put all your source code in it. I'll compile your code and check whether the jars you submit is the same.
**

CLI commands to execute operations: 

Closest Pair
./bin/spark-submit --class src.main.java.edu.asu.cse512.ClosestPair --master spark://192.168.136.131:7077 /home/worker/workspace/fullProjectAssembly/target/closestPair-0.1.jar hdfs://192.168.136.131:54310/FinalTestCases/ClosestPairTestData.csv hdfs://192.168.136.131:54310/GeometricClosestPairOutput.csv

Farthest Pair
./bin/spark-submit --class src.main.java.edu.asu.cse512.FarthestPair --master spark://192.168.136.131:7077 /home/worker/workspace/fullProjectAssembly/target/farthestPair-0.1.jar "hdfs://192.168.136.131:54310/FinalTestCases/FarthestPairTestData.csv" "hdfs://192.168.136.131:54310/GeometricFarthestPairOutput.csv"

