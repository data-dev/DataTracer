import uuid
from random import randint

import pandas as pd

users, posts = [], []
for user_id in range(1000):
    user = {}
    user["id"] = user_id
    user["age"] = randint(18, 100)
    user["birthyear"] = 2020 - user["age"]
    user["height"] = randint(150, 200)
    user["nb_posts"] = randint(0, 10)
    for _ in range(user["nb_posts"]):
        posts.append({
            "id": len(posts),
            "uid": user["id"],
            "text": str(uuid.uuid1())
        })
    users.append(user)

pd.DataFrame(
    users,
    columns=["id", "age", "birthyear", "height", "nb_posts"]
).to_csv("users.csv", index=False)

pd.DataFrame(
    posts,
    columns=["id", "uid", "text"]
).to_csv("posts.csv", index=False)
