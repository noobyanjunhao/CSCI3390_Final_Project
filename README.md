# Large Scale Data Processing: Final Project
For the final project, you are provided 6 CSV files, each containing an undirected graph, which can be found [here](https://drive.google.com/file/d/1khb-PXodUl82htpyWLMGGNrx-IzC55w8/view?usp=sharing). The files are as follows:  

|           File name           |        Number of edges       |
| ------------------------------| ---------------------------- |
| com-orkut.ungraph.csv         | 117185083                    |
| twitter_original_edges.csv    | 63555749                     |
| soc-LiveJournal1.csv          | 42851237                     |
| soc-pokec-relationships.csv   | 22301964                     |
| musae_ENGB_edges.csv          | 35324                        |
| log_normal_100.csv            | 2671                         |  


You can choose to work on **matching** or **correlation clustering**. 

## Matching

Your goal is to compute a matching as large as possible for each graph. 

### Input format
Each input file consists of multiple lines, where each line contains 2 numbers that denote an undirected edge. For example, the input below is a graph with 3 edges.  
1,2  
3,2  
3,4  

### Output format
Your output should be a CSV file listing all of the matched edges, 1 on each line. For example, the ouput below is a 2-edge matching of the above input graph. Note that `3,4` and `4,3` are the same since the graph is undirected.  
1,2  
4,3  

## Correlation Clustering

Your goal is to compute a clustering that has disagreements as small as possible for each graph. 

### Input format
Each input file consists of multiple lines, where each line contains 2 numbers that denote an undirected edge. For example, the input below is a graph with 3 (positive) edges.  
1,2  
3,2  
3,4  

The 3 remaining pairs of vertices that do not appear in the above list denote negative edges. They are (2,4), (1,4), (1,3).

### Output format
Your output should be a CSV file describing all of the clusters. The number of lines should be equal to the number of vertices. Each line consists two numbers, the vertex ID and the cluster ID.

For example, the output below denotes vertex 1, vertex 3, and vertex 4 are in one cluster and vertex 2 forms a singleton cluster.  The clustering has a 4 disagreements.  
1,100  
2,200  
4,100  
3,100  


## No template is provided
For the final project, you will need to write everything from scratch. Feel free to consult previous projects for ideas on structuring your code. That being said, you are provided a verifier that can confirm whether or not your output is a matching or a clustering. As usual, you'll need to compile it with
```
sbt clean package
```  
### Matching

The matching verifier accepts 2 file paths as arguments, the first being the path to the file containing the initial graph and the second being the path to the file containing the matching. It can be ran locally with the following command (keep in mind that your file paths may be different):
```
// Linux
spark-submit --master local[*] --class final_project.matching_verifier target/scala-2.12/project_3_2.12-1.0.jar /data/log_normal_100.csv data/log_normal_100_matching.csv

// Unix
spark-submit --master "local[*]" --class "final_project.matching_verifier" target/scala-2.12/project_3_2.12-1.0.jar data/log_normal_100.csv data/log_normal_100_matching.csv
```

### Correlation Clustering

The clustering verifier accepts 2 file paths as arguments, the first being the path to the file containing the initial graph and the second being the path to the file describing the clustering. It can be ran locally with the following command (keep in mind that your file paths may be different):
```
// Linux
spark-submit --master local[*] --class final_project.clustering_verifier target/scala-2.12/project_3_2.12-1.0.jar /data/log_normal_100.csv data/log_normal_100_clustering.csv

// Unix
spark-submit --master "local[*]" --class "final_project.clustering_verifier" target/scala-2.12/project_3_2.12-1.0.jar data/log_normal_100.csv data/log_normal_100_clustering.csv

```

## Deliverables
* The output file for each test case.
  * For naming conventions, if the input file is `XXX.csv`, please name the output file `XXX_solution.csv`.
  * You'll need to compress the output files into a single ZIP or TAR file before pushing to GitHub. If they're still too large, you can upload the files to Google Drive and include the sharing link in your report.
* The code you've applied to produce the solutions.
  * You should add your source code to the same directory as the verifiers and push it to your repository.
* A project report that includes the following:
  * A table containing the objective of the solution (i.e. the size of matching or the number of disagreements of clustering) you obtained for each test case. The objectives must correspond to the matchings or the clusterings in your output files.
  * An estimate of the amount of computation used for each test case. For example, "the program runs for 15 minutes on a 2x4 N1 core CPU in GCP." If you happen to be executing mulitple algorithms on a test case, report the total running time.
  * Description(s) of your approach(es) for obtaining the matching or the clustering. It is possible to use different approaches for different cases. Please describe each of them as well as your general strategy if you were to receive a new test case. It is important that your approach can scale to larger cases if there are more machines.
  * Discussion about the advantages of your algorithm(s). For example, does it guarantee a constraint on the number of shuffling rounds (say `O(log log n)` rounds)? Does it give you an approximation guarantee on the quality of the solution? If your algorithm has such a guarantee, please provide proofs or scholarly references as to why they hold in your report.
* A 10-minute presentation during class time on 4/29 (Tue) and 5/1 (Thu).
  * Note that the presentation date is before the final project submission deadline. This means that you could still be working on the project when you present. You may present the approaches you're currently trying. You can also present a preliminary result, like the matchings or the clusterings you have at the moment.

## Grading policy
* Quality of solutions (40%)
  * For each test case, you'll receive at least 70% of full credit if your matching size is at 70% the best answer in the class or if your clustering size is at most 130% of the best in the class.
  * **You will receive a 0 for any case where the verifier does not confirm that your output is a correct.** Please do not upload any output files that do not pass the verifier.
* Project report (35%)
  * Your report grade will be evaluated using the following criteria:
    * Discussion of the merits of your algorithms such as the theoretical merits (i.e. if you can show your algorithm has certain guarantee).
    * The scalability of your approach
    * Depth of technicality
    * Novelty
    * Completeness
    * Readability
* Presentation (15%)
* Formatting (10%)
  * If the format of your submission does not adhere to the instructions (e.g. output file naming conventions), points will be deducted in this category.

## Submission via GitHub
Delete your project's current **README.md** file (the one you're reading right now) and include your report as a new **README.md** file in the project root directory. Have no fear—the README with the project description is always available for reading in the template repository you created your repository from. For more information on READMEs, feel free to visit [this page](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/about-readmes) in the GitHub Docs. You'll be writing in [GitHub Flavored Markdown](https://guides.github.com/features/mastering-markdown). Be sure that your repository is up to date and you have pushed all of your project's code. When you're ready to submit, simply provide the link to your repository in the Canvas assignment's submission.

## You must do the following to receive full credit:
1. Create your report in the ``README.md`` and push it to your repo.
2. In the report, you must include your teammates' full name in addition to any collaborators.
3. Submit a link to your repo in the Canvas assignment.

## Deadline and early submission bonus
1. The deadline of the final project is on 5/4 (Sunday) 11:59PM.  
2. **If you submit by 5/2 (Friday) 11:59PM, you will get 5% boost on the final project grade.**  
3. The submission time is calculated from the last commit in the Git log.  
4. **No extension beyond 5/4 11:59PM will be granted, even if you have unused late days.**  
