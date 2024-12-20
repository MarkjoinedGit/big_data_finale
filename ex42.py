import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession

def split_question(line):
    """
    Splits a question line into (QuestionId, TextOfTheQuestion).
    """
    parts = line.split(",", 2)
    return parts[0], parts[2]

def split_answer(line):
    """
    Splits an answer line into (QuestionId, TextOfTheAnswer).
    """
    parts = line.split(",", 3)
    return parts[1], parts[3]

def format_output(question_id, question, answers):
    """
    Formats the output as QuestionId, ([TextOfTheQuestion], [list of answers]).
    """
    formatted_question = f"[{question}]"
    formatted_answers = f"[{', '.join(answers)}]"
    return f"{question_id},({formatted_question},{formatted_answers})"

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Exercise-42").getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input paths
    questions_file = os.path.join(absolute_path, 'input', 'input_42', 'questions.csv')
    answers_file = os.path.join(absolute_path, 'input', 'input_42', 'answers.csv')

    # Output path
    output_path = os.path.join(absolute_path, 'output', 'exercise-42')

    # Create SparkContext
    sc = spark.sparkContext

    # Read questions and answers files
    questions_rdd = sc.textFile(questions_file).map(split_question)
    answers_rdd = sc.textFile(answers_file).map(split_answer)

    # Join questions with their answers
    questions_with_answers = questions_rdd.leftOuterJoin(answers_rdd) \
        .groupByKey() \
        .mapValues(lambda x: (list(set([q for q, _ in x]))[0], [a for _, a in x if a]))

    # Format the output
    formatted_output = questions_with_answers.map(lambda x: format_output(x[0], x[1][0], x[1][1]))

    # Save the result to the output file
    formatted_output.coalesce(1).saveAsTextFile(output_path)

    # Stop Spark session
    spark.stop()
