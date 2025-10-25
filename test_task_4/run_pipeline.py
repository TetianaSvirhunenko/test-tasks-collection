from src.api_client import DummyJsonAPI
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
import os
import sys
import json

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# create a user
api = DummyJsonAPI("https://dummyjson.com")
new_user = {
    "firstName": "Tetiana",
    "lastName": "Svirhunenko",
    "age": 34,
}
api.create_user(new_user)

# log in
credentials = {
    "username": 'emilys', 
    "password": 'emilyspass'
}

api.login(credentials)

# fetch all users and all posts
users = api.get_paginated('/users')
posts = api.get_paginated('/posts')

# female users and all posts written by these users
female_users = api.get_filtered_users('gender', 'female')
female_users_post = api.get_posts_by_users(female_users)

# merge posts with users
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

with open('user_schema.json', 'r', encoding='utf-8') as json_file:
    schema_json = json.load(json_file)
user_schema = StructType.fromJson(schema_json)

users_df = spark.createDataFrame(users, schema=user_schema)
posts_df = spark.createDataFrame(posts)

print(f"Кількість рядків users: {users_df.count()}")
print(f"Кількість рядків posts: {posts_df.count()}")

users_df = users_df.withColumnRenamed("id", "user_id")
posts_df = posts_df.withColumnRenamed("id", "post_id")

merged_df = posts_df.join(users_df, posts_df.userId == users_df.user_id, "inner")

# top 10 posts with the highest like counts

likes_per_post = merged_df.select("post_id", F.col("reactions")["likes"].alias("likes")) \
    .orderBy(F.col("likes").desc()) \
    .limit(10)

likes_per_post.show(truncate=False)

# the top 10 users with the highest total number of likes
likes_per_user = merged_df.groupBy("userId") \
                   .agg(F.sum(F.col("reactions.likes")).alias("total_likes")) \
                   .orderBy(F.col("total_likes").desc()) \
                   .limit(10)

likes_per_user.show(truncate=False)

# 1. How many posts each user has created
posts_per_user = merged_df.groupBy("userId") \
                    .agg(F.count("*").alias("posts_count"))

posts_per_user.write.mode("overwrite").csv("file:///C:/Users/Tanya/Desktop/Genome/dataset/posts_per_user", header=True)

# 2. The average number of likes per post for each use
avg_likes_per_user = merged_df.groupBy("userId") \
                    .agg(F.avg(F.col("reactions.likes")).alias("avg_likes")) \
                    .orderBy(F.col("avg_likes").desc())

avg_likes_per_user.write.mode("overwrite").csv("file:///C:/Users/Tanya/Desktop/Genome/dataset/avg_likes_per_user", header=True)

# 3. How many users haven't written any posts
users_without_posts = users_df.join(posts_df, posts_df.userId == users_df.user_id, how="left_anti") \
                    .select("user_id")

users_without_posts.write.mode("overwrite").csv("file:///C:/Users/Tanya/Desktop/Genome/dataset/users_without_posts", header=True)

# 4. What are the top 3 cities where users live that received the highest total number of likes from their posts?
likes_by_city = merged_df.groupBy(F.col("address.city").alias("city")) \
                    .agg(F.sum(F.col("reactions.likes")).alias("total_likes")) \
                    .orderBy(F.col("total_likes").desc()) \
                    .limit(3)

likes_by_city.write.mode("overwrite").csv("file:///C:/Users/Tanya/Desktop/Genome/dataset/likes_by_city", header=True)

#5. Find anomalies in likes per user.
quantiles = avg_likes_per_user.approxQuantile("avg_likes", [0.25, 0.75], 0.01)
Q1, Q3 = quantiles
IQR = Q3 - Q1
lower_bound = max(0, Q1 - 1.5 * IQR)
upper_bound = Q3 + 1.5 * IQR

anomalies = avg_likes_per_user.filter(
    (F.col("avg_likes") >= F.lit(upper_bound)) |
    (F.col("avg_likes") <= F.lit(lower_bound))
)

anomalies.write.mode("overwrite").csv("file:///C:/Users/Tanya/Desktop/Genome/dataset/anomalies", header=True)

spark.stop()
