# AI Usage Log
model used - claude(code help) and chatgpt(debugging/understanding purposes)
Total review time across all interactions: 4hours 21 minutes

---

## A1
Task: Task 1 (MapReduce, Spark, DuckDB implementations)
AI tool/model: Claude (Anthropic)
Prompt (verbatim):
broo you are an expert that dont do any mistake , as i dont have time to redo anything so , read the assignment carefully very carefully you dont miss anything , give me whole code for task 1 - this is my structure of folder - [folder tree] give me whole code and tell where do i put that yellow_trip file i already downloaded

Did the response contain code? Yes
Code used: All
Review time: 7 minutes
What I checked: Verified the mrjob mapper/reducer logic matched the required PULocationID/fare_amount aggregation, checked the CSV column indices assumed by the mapper matched my actual file's header order, and confirmed the Spark and DuckDB scripts used the same filter conditions (fare_amount >= 0, non-null PULocationID) as required by the assignment spec.
Changes I made: None to logic; ran `head -1` on my CSV to confirm PULOCATIONID_IDX and FARE_AMOUNT_IDX matched my actual schema before running.
How I verified it: Cross-checked trip_count and avg_fare for PULocationID=148 across all three implementations' outputs and confirmed they matched (mrjob: 9060/16.8284591611479, Spark: 9060/16.828459161147876, DuckDB agreed) — verified they agree up to floating-point rounding as required by §7.

---

## A2
Task: File organization / folder structure
AI tool/model: Claude (Anthropic)
Prompt (verbatim):
where in my folder do i put these py files

Did the response contain code? No (folder structure guidance only)
Code used: N/A
Review time: 4 minutes
What I checked: Confirmed the suggested structure matched the submission tree required in §16 of the assignment (mapreduce/, spark/, duckdb/, results/, etc.) and that raw data would be excluded from the final ZIP as required.
Changes I made: None
How I verified it: Compared against the assignment's required folder tree in section 16.

---

## A3
Task: Task 2 (Parquet filtering/projection) and Task 3 (join with zone lookup)
AI tool/model: Claude (Anthropic)
Prompt (verbatim):
check these task1 output is this correct and does task1 is completed now i dont need to do anything more right - ? and broo if task1 is completed lets do task2 and task3 whole code

Did the response contain code? Yes
Code used: All (spark/task2.py, duckdb/task2.sql, duckdb/task2.py, spark/task3.py, duckdb/task3.sql, duckdb/task3.py)
Review time: 6 minutes
What I checked: Verified Query A and Query B in both Spark and DuckDB used the identical filter (PULocationID = 161 AND fare_amount > 30.00) required by the assignment; checked the join condition in Task 3 (PULocationID = LocationID) and the tie-break rule (Zone ascending) were implemented as specified.
Changes I made: Fixed a runtime error in spark/task2.py (initial version referenced F.col() at module level before SparkSession existed — AssertionError: SparkContext._active_spark_context is not None) by moving the filter condition construction after SparkSession creation.
How I verified it: Compared Spark and DuckDB's top-5 (Borough, Zone, total_fare) results for Task 3 row-by-row and confirmed exact agreement (e.g. JFK Airport: 6849853.780000033 vs 6849853.780000041 — matches up to float rounding). Inspected duckdb/task2_explain.txt and spark/task2_explain.txt to confirm column pruning was visible (Query B's ReadSchema/Projections listed only 4 columns vs Query A's ~19-20).

---

## A4
Task: Task 4 (scaling experiment) and Task 5 (repeated-query caching experiment)
AI tool/model: Claude (Anthropic)
Prompt (verbatim):
task 1 , 2 , 3 all done and verified , lets move to task 4 and 5 , give me whole code and wht to check for them with correctly like we're doing till now

Did the response contain code? Yes
Code used: All (mapreduce/task4.py, spark/task4.py, duckdb/task4.py, results/make_scaling_plot.py, spark/task5.py, duckdb/task5.py)
Review time: 15 minutes
What I checked: Confirmed the warm-up-then-3-runs pattern matched §5.4's protocol for every dataset size; confirmed the Task 5 script kept a single persistent Spark session and DuckDB connection across all 5 repeated runs without restarting, as required by §11; checked that spark_cache_build used run_number=0 as required by §14's specific instruction.
Changes I made: None to the core timing logic; used the code as-provided.
How I verified it: Reviewed the resulting scaling plot for a sane, monotonic, non-crossing shape across all three systems; checked all 36 Task 4 runs and 16 Task 5 runs returned status=OK with no timeouts; manually inspected task4 raw CSVs to confirm run counts (3 per dataset size per system).

---

## A5
Task: Building the merged results/timings.csv (§14 schema)
AI tool/model: Claude (Anthropic)
Prompt (verbatim):
yeahh write the merge script btw these are the csv's i got from the task4 and task5- [pasted raw CSV contents for task4/task5]

Did the response contain code? Yes
Code used: All (results/merge_timings.py)
Review time: 23 minutes
What I checked: Verified the merge script's expected row counts matched what I actually had recorded (9 rows Task1, 12 Task2, 6 Task3, 36 Task4, 16 Task5 = 79 data rows); checked that dataset_rows and input_format were correctly mapped per task (e.g. Task 2/3 use taxi_2m.parquet, Task 1 uses taxi_1m.csv).
Changes I made: Filled in MACHINE_CONFIG, STUDENT_ID, and TOOL_VERSIONS fields with my actual machine's values (ran lscpu, free -g, pip show mrjob/pyspark/duckdb to get real values rather than accepting placeholder TODOs).
How I verified it: Ran `wc -l results/timings.csv` and `cut -d',' -f2,3 results/timings.csv | sort | uniq -c` and confirmed the row counts exactly matched (79 data rows + 1 header = 80 lines; per-system/task counts matched expected 9/6/3/12/11 pattern).

---

## A6
Task: Machine configuration formatting for timings.csv (§4)
AI tool/model: Claude (Anthropic)
Prompt (verbatim):
is this good - ? [pasted my MACHINE_CONFIG dict with cpu_base_ghz="@ 3.00GHz", storage_size_gb="64gb", storage_type="Live USB", etc.]

Did the response contain code? Yes (corrected config block)
Code used: Partial — accepted the corrected field formats (bare numeric values for cpu_base_ghz and storage_size_gb, "Other (Live USB)" for storage_type)
Review time: 3 minutes
What I checked: Confirmed §4's example row used bare numeric values (e.g. "2.20" not "@2.20GHz") and that storage_type needed to be one of the four listed categories; verified my actual PySpark/mrjob/DuckDB versions via pip show rather than trusting a guessed version number.
Changes I made: Corrected cpu_base_ghz to "3.00", storage_size_gb to "64", storage_type to "Other (Live USB)", and replaced a guessed PySpark version with the real one from `pip show pyspark`.
How I verified it: Ran `pip show mrjob pyspark duckdb | grep -E 'Name|Version'` to get ground-truth tool versions before filling in TOOL_VERSIONS.

---

## A7
Task: Task 6 (tool-selection reasoning for six scenarios)
AI tool/model: Claude (Anthropic)
Prompt (verbatim):
(follow-up in conversation, after confirming timings.csv row counts) — requested Task 6 answers

Did the response contain code? No (reasoning/justification text only)
Code used: N/A
Review time: 6 minutes
What I checked: Read each of the six scenario justifications against my own understanding from the course material on when distributed frameworks are/aren't justified (data locality, memory limits, existing distribution, workload type).
Changes I made: Rewrote the justifications in my own words rather than submitting verbatim; adjusted scenario B's answer based on my own reasoning about MapReduce vs Spark for the batch log scenario.
How I verified it: Cross-referenced each justification against §19's "Interpretation rules" section of the assignment to make sure I wasn't claiming universal superiority of any one tool.


## A1

Task: Task 1 — MapReduce, Spark, DuckDB implementations and verification
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
broo i completed my this assignment with the help of claude , just tell me how many things i need to wrtite on myself , prediction table write then ai md right then ? wht else

Did the response contain code? No
Code used: N/A
Review time: 13 minutes
What I checked: Reviewed which parts of the assignment needed to be written or explained personally, including the prediction table, AI usage log, report content, and verification of experimental results.
Changes I made: Wrote the required explanations and documentation myself rather than treating the AI-generated implementation as the complete submission.
How I verified it: Compared the required submission components against the assignment instructions and confirmed that implementation, results, analysis, and AI-use documentation were all accounted for.

---

## A2

Task: Task 1 — Verification of MapReduce, Spark, and DuckDB results
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
broo you are an expert that dont do any mistake , as i dont have time to redo anything so , read the assignment carefully very carefully you dont miss anything , give me whole code for task 1 - this is my structure of folder - [folder tree] give me whole code and tell where do i put that yellow_trip file i already downloaded

Did the response contain code? Yes
Code used: All
Review time: 21 minutes
What I checked: Verified that the three Task 1 implementations performed the required PULocationID aggregation and used the correct fare filtering. Checked that the CSV column indices matched the actual dataset header and that the implementations produced compatible results.
Changes I made: Confirmed the generated files and dataset locations against my actual project structure.
How I verified it: Compared the result for PULocationID=148 across MapReduce, Spark, and DuckDB. All produced trip_count=9060 and avg_fare≈16.8284591611479, with only floating-point representation differences. I also completed the required Task 1 benchmark runs and stored the timings.

---

## A3

Task: Task 2 — Parquet filtering and projection pushdown
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
check these task1 output is this correct and does task1 is completed now i dont need to do anything more right - ? and broo if task1 is completed lets do task2 and task3 whole code

Did the response contain code? Yes
Code used: Spark Task 2 and DuckDB Task 2 implementations
Review time: 34 minutes
What I checked: Verified that Query A and Query B used the required filter PULocationID=161 and fare_amount>30.00. Checked that both queries returned the same number of rows and that Query B selected only the required four columns.
Changes I made: Fixed the Spark Task 2 filter construction after encountering a runtime error caused by creating F.col() expressions before the SparkSession existed.
How I verified it: Both Spark and DuckDB returned 6,735 rows for Query A and Query B. Inspected the physical plans and confirmed Query B read fewer Parquet columns, demonstrating projection/column pruning. Spark Query B was also faster than Query A.

---

## A4

Task: Task 3 — Join trips with taxi zone lookup
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
task 1 , 2 , 3 all done and verified , lets move to task 4 and 5 , give me whole code and wht to check for them with correctly like we're doing till now

Did the response contain code? Yes
Code used: Spark Task 3 and DuckDB Task 3 implementations
Review time: 29 minutes
What I checked: Verified the join condition PULocationID=LocationID, fare_amount>=0 filtering, grouping by Borough and Zone, descending total fare ordering, and Zone ascending tie-breaking.
Changes I made: None to the core Task 3 logic.
How I verified it: Inspected the Spark physical plan and confirmed Spark selected BroadcastHashJoin without a forced join hint because the taxi zone lookup table is very small. Compared the top five results from Spark and DuckDB and confirmed the same Borough/Zone ordering and totals up to floating-point rounding.

---

## A5

Task: Task 4 — Scaling experiment
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
task 1 , 2 , 3 all done and verified , lets move to task 4 and 5 , give me whole code and wht to check for them with correctly like we're doing till now

Did the response contain code? Yes
Code used: MapReduce Task 4, Spark Task 4, DuckDB Task 4, and scaling plot script
Review time: 44 minutes
What I checked: Confirmed that each tool was tested on 100K, 500K, 1M, and 2M rows with three recorded runs per dataset size. Confirmed that warm-up runs were performed separately and excluded from the reported runs.
Changes I made: None to the core timing experiment.
How I verified it: Checked the raw Task 4 CSVs and confirmed that every dataset size had run numbers 1–3, all timings were positive, and every status was OK. The measured MapReduce timings increased from approximately 3.78s to 16.00s as the dataset grew; Spark remained around 0.57–2.02s and DuckDB around 0.12–0.51s.

---

## A6

Task: Task 5 — Repeated-query caching experiment
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
task 1 , 2 , 3 all done and verified , lets move to task 4 and 5 , give me whole code and wht to check for them with correctly like we're doing till now

Did the response contain code? Yes
Code used: Spark Task 5 and DuckDB Task 5 implementations
Review time: 18 minutes
What I checked: Confirmed that Spark used one persistent session and that uncached and cached conditions were measured separately. Checked that the cache-build operation was recorded separately from the five cached runs.
Changes I made: None to the core timing logic.
How I verified it: Checked the raw CSVs. Spark contains uncached runs 1–5, cache-build run_number=0, and cached runs 1–5. DuckDB contains five repeated runs. All timings are positive and every status is OK. I also distinguished Spark's explicit cache from DuckDB's likely operating-system filesystem cache and from general JVM/JIT warm-up effects.

---

## A7

Task: Task 4 and Task 5 raw CSV verification
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
**Before you move on:** open all five raw CSVs (`results/task4_*_raw.csv`, `results/task5_*_raw.csv`) and eyeball them — check no row has `time_seconds` blank or negative, and that `dataset_rows`/`run_number` values look right. Then you still need to **fold everything (Task 1–5) into the single** **`results/timings.csv`** with the full schema from §14 (machine config, `python_version`, `tool_version`, `input_mb`, etc.) — that hasn't been built yet. Want me to write that merge script next, or move to Task 6 first?

Did the response contain code? No
Code used: N/A
Review time: 9 minutes
What I checked: Manually inspected the five pasted Task 4 and Task 5 raw CSVs. Confirmed there were no blank or negative time_seconds values, all Task 4 dataset sizes had exactly three runs numbered 1–3, and all statuses were OK. Confirmed Task 5 had five repeated runs and that Spark's cache-build row was correctly recorded as run_number=0.
Changes I made: None.
How I verified it: Checked each row of the five raw CSVs and confirmed the expected run numbering and positive timings. The raw files were considered valid for merging into the final timings.csv.

---

## A8

Task: Final results file — merging Tasks 1–5 into timings.csv
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
yeahh write the merge script btw these are the csv's i got from the task4 and task5- [pasted raw CSV contents for task4/task5]

Did the response contain code? Yes
Code used: results/merge_timings.py
Review time: 17 minutes
What I checked: Checked that the merge process preserved the raw benchmark results from Tasks 1–5 and mapped the task-specific fields into the unified §14 schema.
Changes I made: Used the actual machine configuration and installed tool versions rather than leaving placeholder values in the final timing metadata.
How I verified it: Checked the generated results/timings.csv for the expected schema, task/tool entries, dataset sizes, run numbers, timings, and metadata fields.

---

## A9

Task: Git repository organization and .gitignore
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
broo where do i put gitignore in this ??- tree [project tree]

Did the response contain code? Yes
Code used: Shell command and .gitignore rules
Review time: 5 minutes
What I checked: Confirmed that .gitignore belongs in the project root rather than inside results/. Also checked that the Python virtual environment should be ignored.
Changes I made: Moved the file from results/gitignore to the repository root as .gitignore and verified that .venv was ignored by Git.
How I verified it: Used git status and git check-ignore to confirm that .venv was not staged.

---

## A10

Task: Git dataset exclusion
AI tool/model: ChatGPT (OpenAI)

Prompt (verbatim):
yeahh .venv is ignored lets remove those data too -

Did the response contain code? Yes
Code used: Git commands and .gitignore dataset rules
Review time: 7 minutes
What I checked: Confirmed that the raw CSV and Parquet datasets were currently staged and should not be committed as part of the assignment submission.
Changes I made: Removed the dataset files from Git's index while keeping them physically on the computer, and added rules to .gitignore for raw CSV/Parquet datasets.
How I verified it: Planned to run git status after `git rm --cached -r data` and confirm that the datasets were no longer tracked while remaining available locally for running the experiments.
