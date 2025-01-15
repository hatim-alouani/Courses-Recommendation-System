import warnings
import math
import pyspark
import numpy as np
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from flask import Flask, render_template, request, jsonify
import os
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

warnings.filterwarnings("ignore", message="Signature.*does not match any known type")

app = Flask(__name__)

spark = SparkSession.builder \
    .appName("Spark") \
    .master("local[1]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.warehouse.dir", "/tmp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('courses')

def insert_recommendation(user_id, course_id, course_name, predicted_rating):
    session.execute(
        """
        INSERT INTO recommandationOfCourses (user_id, course_id, course_name, predicted_rating)
        VALUES (%s, %s, %s, %s)
        """,
        (user_id, course_id, course_name, predicted_rating)
    )

def clip_prediction(predicted_rating, min_rating=1, max_rating=5):
    mi = min(predicted_rating, max_rating)
    ret = max(mi, min_rating)
    return (ret)

def get_and_save_recommendations(user_id):
    os.system("clear")
    print(f"Getting recommendations for User ID: {user_id}")
    
    dataFrame = spark.read.format('csv') \
        .option('inferSchema', True) \
        .option('header', True) \
        .load('hdfs://localhost:9000/spark/final_data2.csv')
    
    dataFrame = dataFrame.select('user_id', 'course_id', 'rate', 'title')
    
    if dataFrame.filter(col("user_id") == user_id).count() == 0:
        print("User not found in dataset!")
        return [], False

    trainSet, testSet = dataFrame.randomSplit([0.8, 0.2], seed=42)
    
    als = pyspark.ml.recommendation.ALS(userCol="user_id", itemCol="course_id", ratingCol="rate", coldStartStrategy="drop", nonnegative=True)
    model = als.fit(trainSet)

    course_ids = dataFrame.select("course_id").distinct()
    user_data = course_ids.withColumn("user_id", lit(user_id))
    user_predictions = model.transform(user_data)

    rated_courses = dataFrame.filter(col("user_id") == user_id).select("course_id").distinct()
    unrated_courses = user_predictions.join(rated_courses, "course_id", "left_anti")

    recommended_courses = unrated_courses.orderBy(col("prediction").desc()).limit(5).collect()

    recommendations = []

    for row in recommended_courses:
        course_info = dataFrame.select('course_id', 'title')
        course = course_info.filter(col("course_id") == row.course_id).collect()
        course_name = course[0].title if course else 'Unknown Course'

        insert_recommendation(user_id, row.course_id, course_name, row.prediction)

        recommendations.append({
            "course_id": row.course_id,
            "course_name": course_name,
            "rating": row.prediction
        })

    return recommendations, True

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/recommendations', methods=['GET'])
def recommendations():
    try:
        user_id = int(request.args.get('user_id'))
        recommended_courses, user_found = get_and_save_recommendations(user_id)

        if not user_found:
            return jsonify({"message": "User not found in the dataset"}), 404

        recommended_courses = sorted(recommended_courses, key=lambda x: x['rating'], reverse=True)
        return jsonify(recommended_courses)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"message": "Error processing the request"}), 500

if __name__ == '__main__':
    app.run(debug=True)
