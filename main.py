# Importing flask module in the project is mandatory
# An object of Flask class is our WSGI application.
import os
from typing import List, Tuple

import boto3
import mysql.connector
import tabulate
from flask import Flask, json, jsonify, request
from flask_cors import CORS  # Import the CORS module
from SNSWrapper import SnsWrapper

# Flask constructor takes the name of
# current module (__name__) as argument.
app = Flask(__name__)
# Enable CORS for all routes and origins (adjust for production)
CORS(app, resources={r"/*": {"origins": "http://127.0.0.1:5002"}})
TOPIC_NAME: str = "GameSub"
SERVICE_NAME: str = "sns"
REGION_NAME: str = os.getenv("REGION_NAME")
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN: str = os.getenv("AWS_SESSION_TOKEN")
HOST: str = os.getenv("DB_HOST")
USER: str = os.getenv("DB_USER")
PASSWORD: str = os.getenv("DB_PASSWORD")
DATABASE: str = os.getenv("DB_DATABASE")


def sendEmail() -> None:
    sns_wrapper = SnsWrapper(
        boto3.resource(
            service_name=SERVICE_NAME,
            region_name=REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
        )
    )

    # Create an SNS topic if it doesn't exist
    # if TOPIC_NAME not in [i.arn.split(":")[-1] for i in sns_wrapper.list_topics()]:
    topic = sns_wrapper.create_topic(TOPIC_NAME)

    # Construct the message
    message: str = "The highscores have been updated. The new highscores are:\n\n"
    message += "_" * 20
    current_scores, current_users = getCurrentTopScores()
    message += f"\n{tabulate.tabulate(list(zip(current_users, current_scores)), headers=['User', 'Score'])}\n"

    # Send the message
    sns_wrapper.publish_message(topic=topic, message=message, attributes={})

    return None


def subscribe(email: str) -> str:
    sns_wrapper = SnsWrapper(
        boto3.resource(
            service_name=SERVICE_NAME,
            region_name=REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
        )
    )

    # Create an SNS topic if it doesn't exist
    # if TOPIC_NAME not in [i.arn.split(":")[-1] for i in sns_wrapper.list_topics()]:
    topic = sns_wrapper.create_topic(TOPIC_NAME)

    # Subscribe an email to a topic
    # Get all the subscriptions that are completed
    subs = [
        i
        for i in sns_wrapper.list_subscriptions(topic=topic)
        if i.arn != "PendingConfirmation"
    ]

    message: str
    # Check if the email provided is already subscribed
    if email in [sub.attributes["Endpoint"] for sub in subs]:
        # If it is already subscribed we do nothig
        message = "The email is already subscribed."
    else:
        # Else we send a subscription email
        _ = sns_wrapper.subscribe(topic=topic, protocol="email", endpoint=email)
        message = "A subscription email has been sent."

    return message


@app.route("/subscribe", methods=["POST"])
def subscribeRequest():

    request_data = request.json

    email: str = request_data["data"]["user_email"]

    message: str = subscribe(email=email)

    data = {"error": "none", "command": "subscribe", "message": message}

    response = app.response_class(
        response=json.dumps(data), status=200, mimetype="application/json"
    )
    return response


def getCurrentTopScores() -> Tuple[List[int], List[str]]:
    # Connect to the database
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DATABASE
    )

    mycursor = mydb.cursor()

    mycursor.execute(
        "SELECT username, score FROM highscores ORDER BY score DESC LIMIT 10"
    )

    myresult = mycursor.fetchall()
    current_scores: List[int] = [result[1] for result in myresult]
    current_users: List[int] = [result[0] for result in myresult]
    return current_scores, current_users


def selectiveSendEmail(
    current_scores: List[int], new_score: int, current_users: List[str], new_user: str
) -> bool:

    if len(current_scores) < 10:
        sendEmail()
        return True
    elif new_score > current_scores[-1]:
        # We beat the minimum score
        sendEmail()
        return True

    return False


@app.route("/updates", methods=["POST"])
def update():
    request_data = request.json

    current_scores: List[int]
    current_users: List[str]
    current_scores, current_users = getCurrentTopScores()

    email_sent: bool = selectiveSendEmail(
        current_scores=current_scores,
        current_users=current_users,
        new_score=request_data["data"]["score"],
        new_user=request_data["data"]["username"],
    )
    data = {
        "error": "none",
        "message": "Email sent" if email_sent else "No update necessary",
        "command": "updated_scores",
    }
    response = app.response_class(
        response=json.dumps(data), status=200, mimetype="application/json"
    )
    return response


# main driver function
if __name__ == "__main__":

    # run() method of Flask class runs the application
    # on the local development server.
    app.run(host="0.0.0.0", port=8000)
