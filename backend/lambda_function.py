import json
import boto3
import base64
import uuid
from datetime import datetime, timezone

from boto3.dynamodb.conditions import Attr


# =================================================
# AWS SERVICES
# =================================================

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
rekognition = boto3.client("rekognition")
polly = boto3.client("polly")
sns = boto3.client("sns")


# =================================================
# CONFIGURATION
# =================================================

BUCKET_NAME = "yaman-complaint-images"
TABLE_NAME = "Complaints"

SNS_TOPIC_ARN = (
    "arn:aws:sns:ap-south-1:"
    "220664822853:"
    "SmartCityComplaintAlerts"
)

table = dynamodb.Table(TABLE_NAME)


# =================================================
# MAIN LAMBDA HANDLER
# =================================================

def lambda_handler(event, context):

    try:

        # =================================================
        # GET HTTP METHOD
        # =================================================

        method = event.get(
            "requestContext",
            {}
        ).get(
            "http",
            {}
        ).get(
            "method",
            ""
        )

        print("HTTP Method:", method)


        # =================================================
        # GET /complaints
        # =================================================

        if method == "GET":

            print(
                "Fetching complaints from DynamoDB..."
            )

            result = table.scan()

            complaints = result.get(
                "Items",
                []
            )

            while "LastEvaluatedKey" in result:

                result = table.scan(
                    ExclusiveStartKey=result[
                        "LastEvaluatedKey"
                    ]
                )

                complaints.extend(
                    result.get(
                        "Items",
                        []
                    )
                )

            # -------------------------------------------------
            # Sort newest complaints first
            # -------------------------------------------------

            complaints.sort(
                key=lambda x: x.get(
                    "createdAt",
                    ""
                ),
                reverse=True
            )

            # -------------------------------------------------
            # Generate image URLs
            # -------------------------------------------------

            for complaint in complaints:

                s3_key = complaint.get(
                    "s3Key"
                )

                if s3_key:

                    try:

                        image_url = (
                            s3.generate_presigned_url(
                                "get_object",
                                Params={
                                    "Bucket": BUCKET_NAME,
                                    "Key": s3_key
                                },
                                ExpiresIn=3600
                            )
                        )

                        complaint[
                            "imageUrl"
                        ] = image_url

                    except Exception as image_error:

                        print(
                            "Could not generate image URL:",
                            str(image_error)
                        )

                        complaint[
                            "imageUrl"
                        ] = None

                else:

                    complaint[
                        "imageUrl"
                    ] = None

            print(
                "Total complaints found:",
                len(complaints)
            )

            return response(
                200,
                {
                    "message":
                        "Complaints fetched successfully",

                    "count":
                        len(complaints),

                    "complaints":
                        complaints
                }
            )


        # =================================================
        # PATCH /complaints
        # =================================================

        if method == "PATCH":

            print(
                "Updating complaint status..."
            )

            body = event.get(
                "body",
                event
            )

            if isinstance(
                body,
                str
            ):

                body = json.loads(body)

            complaint_id = body.get(
                "complaintId"
            )

            new_status = body.get(
                "status"
            )

            print(
                "Complaint ID:",
                complaint_id
            )

            print(
                "New status:",
                new_status
            )

            if not complaint_id:

                return response(
                    400,
                    {
                        "error":
                            "complaintId is required"
                    }
                )

            allowed_statuses = [
                "Pending",
                "In Progress",
                "Resolved"
            ]

            if new_status not in allowed_statuses:

                return response(
                    400,
                    {
                        "error":
                            "Invalid status",

                        "allowedStatuses":
                            allowed_statuses
                    }
                )

            result = table.update_item(

                Key={
                    "complaintId":
                        complaint_id
                },

                UpdateExpression=
                    "SET #status = :status, "
                    "updatedAt = :updatedAt",

                ExpressionAttributeNames={
                    "#status":
                        "status"
                },

                ExpressionAttributeValues={
                    ":status":
                        new_status,

                    ":updatedAt":
                        datetime.now(
                            timezone.utc
                        ).isoformat()
                },

                ReturnValues="ALL_NEW"
            )

            updated_item = result.get(
                "Attributes",
                {}
            )

            print(
                "Complaint status updated successfully"
            )

            return response(
                200,
                {
                    "message":
                        "Complaint status updated successfully",

                    "complaintId":
                        complaint_id,

                    "status":
                        new_status,

                    "updatedAt":
                        updated_item.get(
                            "updatedAt"
                        )
                }
            )


        # =================================================
        # POST /upload
        # =================================================

        if method == "POST":

            body = event.get(
                "body",
                event
            )

            if isinstance(
                body,
                str
            ):

                body = json.loads(body)

            location = body.get(
                "location"
            )

            image_name = body.get(
                "imageName"
            )

            image_base64 = body.get(
                "image"
            )

            # -------------------------------------------------
            # Validate location
            # -------------------------------------------------

            if not location:

                return response(
                    400,
                    {
                        "error":
                            "Location is required"
                    }
                )

            # -------------------------------------------------
            # Validate image name
            # -------------------------------------------------

            if not image_name:

                return response(
                    400,
                    {
                        "error":
                            "Image name is required"
                    }
                )

            # -------------------------------------------------
            # Validate image
            # -------------------------------------------------

            if not image_base64:

                return response(
                    400,
                    {
                        "error":
                            "Image is required"
                    }
                )


            # =================================================
            # GENERATE COMPLAINT ID
            # =================================================

            complaint_id = (
                "CMP-" +
                str(
                    uuid.uuid4()
                )[:8].upper()
            )

            print(
                "Complaint ID:",
                complaint_id
            )


            # =================================================
            # S3 IMAGE KEY
            # =================================================

            s3_key = (
                f"complaints/"
                f"{complaint_id}-"
                f"{image_name}"
            )


            # =================================================
            # DECODE IMAGE
            # =================================================

            image_bytes = base64.b64decode(
                image_base64
            )

            print(
                "Image size:",
                len(image_bytes),
                "bytes"
            )


            # =================================================
            # UPLOAD IMAGE TO S3
            # =================================================

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=image_bytes,
                ContentType="image/jpeg"
            )

            print(
                "Image uploaded to S3:",
                s3_key
            )


            # =================================================
            # AMAZON REKOGNITION
            # =================================================

            rekognition_result = (
                rekognition.detect_labels(
                    Image={
                        "Bytes":
                            image_bytes
                    },

                    MaxLabels=20,

                    MinConfidence=40
                )
            )

            labels = [
                label["Name"]
                for label in
                rekognition_result.get(
                    "Labels",
                    []
                )
            ]

            print(
                "Rekognition labels:",
                labels
            )


            # =================================================
            # DETERMINE COMPLAINT TYPE
            # =================================================

            issue = detect_issue(
                labels
            )

            print(
                "Detected issue:",
                issue
            )


            # =================================================
            # CURRENT UTC TIME
            # =================================================

            created_at = datetime.now(
                timezone.utc
            ).isoformat()


            # =================================================
            # SAVE COMPLAINT TO DYNAMODB
            # =================================================

            table.put_item(
                Item={

                    "complaintId":
                        complaint_id,

                    "location":
                        location,

                    "issue":
                        issue,

                    "rekognitionLabels":
                        labels,

                    "imageName":
                        image_name,

                    "s3Key":
                        s3_key,

                    "status":
                        "Pending",

                    "createdAt":
                        created_at
                }
            )

            print(
                "Complaint saved to DynamoDB:",
                complaint_id
            )


            # =================================================
            # COUNT UNRESOLVED COMPLAINTS
            # =================================================

            print(
                "Counting unresolved complaints for:",
                location
            )

            unresolved_count = 0

            result = table.scan(
                FilterExpression=(
                    Attr("location").eq(location)
                    &
                    Attr("status").ne("Resolved")
                )
            )

            unresolved_count += len(
                result.get(
                    "Items",
                    []
                )
            )

            while "LastEvaluatedKey" in result:

                result = table.scan(
                    ExclusiveStartKey=result[
                        "LastEvaluatedKey"
                    ],

                    FilterExpression=(
                        Attr("location").eq(location)
                        &
                        Attr("status").ne("Resolved")
                    )
                )

                unresolved_count += len(
                    result.get(
                        "Items",
                        []
                    )
                )

            print(
                "Unresolved complaints in",
                location,
                ":",
                unresolved_count
            )


            # =================================================
            # CREATE AUDIO MESSAGE
            # =================================================

            audio_text = (
                f"New complaint received. "
                f"{issue} has been reported at "
                f"{location}. "
                f"There are currently "
                f"{unresolved_count} unresolved "
                f"complaints in this location."
            )

            print(
                "Audio text:",
                audio_text
            )


            # =================================================
            # AMAZON POLLY
            # =================================================

            polly_result = polly.synthesize_speech(

                Text=audio_text,

                OutputFormat="mp3",

                VoiceId="Joanna",

                Engine="standard"
            )

            audio_stream = (
                polly_result[
                    "AudioStream"
                ].read()
            )


            # =================================================
            # S3 AUDIO KEY
            # =================================================

            audio_key = (
                f"complaint-audio/"
                f"{complaint_id}.mp3"
            )


            # =================================================
            # SAVE AUDIO TO S3
            # =================================================

            s3.put_object(

                Bucket=BUCKET_NAME,

                Key=audio_key,

                Body=audio_stream,

                ContentType="audio/mpeg"
            )

            print(
                "Audio uploaded to S3:",
                audio_key
            )


            # =================================================
            # CREATE TEMPORARY AUDIO URL
            # =================================================

            audio_url = (
                s3.generate_presigned_url(

                    "get_object",

                    Params={
                        "Bucket":
                            BUCKET_NAME,

                        "Key":
                            audio_key
                    },

                    ExpiresIn=86400
                )
            )

            print(
                "Audio URL generated"
            )


            # =================================================
            # SNS MESSAGE
            # =================================================

            sns_message = (
                "🔔 NEW SMART CITY COMPLAINT\n\n"
                f"Complaint ID: {complaint_id}\n"
                f"Issue: {issue}\n"
                f"Location: {location}\n"
                f"Unresolved complaints in {location}: "
                f"{unresolved_count}\n\n"
                "🎧 LISTEN TO COMPLAINT AUDIO\n\n"
                f"🔊 Play Audio:\n"
                f"{audio_url}\n\n"
                "The complaint has been recorded successfully."
            )


            # =================================================
            # SEND SNS NOTIFICATION
            # =================================================

            sns.publish(

                TopicArn=SNS_TOPIC_ARN,

                Subject="New Smart City Complaint",

                Message=sns_message
            )

            print(
                "SNS notification sent successfully"
            )


            # =================================================
            # API RESPONSE
            # =================================================

            return response(
                200,
                {
                    "message":
                        "Complaint uploaded successfully",

                    "complaintId":
                        complaint_id,

                    "location":
                        location,

                    "imageName":
                        image_name,

                    "s3Key":
                        s3_key,

                    "issue":
                        issue,

                    "rekognitionLabels":
                        labels,

                    "status":
                        "Pending",

                    "createdAt":
                        created_at,

                    "unresolvedComplaints":
                        unresolved_count,

                    "audioKey":
                        audio_key,

                    "audioUrl":
                        audio_url
                }
            )


        # =================================================
        # UNSUPPORTED METHOD
        # =================================================

        return response(
            405,
            {
                "error":
                    "Method not allowed"
            }
        )


    # =================================================
    # ERROR HANDLING
    # =================================================

    except Exception as e:

        print(
            "ERROR:",
            str(e)
        )

        return response(
            500,
            {
                "error":
                    str(e)
            }
        )


# =================================================
# DETERMINE COMPLAINT TYPE
# =================================================

def detect_issue(labels):

    labels_lower = [
        label.lower()
        for label in labels
    ]


    garbage_keywords = [
        "garbage",
        "waste",
        "trash",
        "litter",
        "rubbish",
        "dump",
        "debris"
    ]


    pothole_keywords = [
        "pothole",
        "road damage",
        "damaged road",
        "hole",
        "puddle",
        "manhole",
        "tar",
        "street"
    ]


    streetlight_keywords = [
        "streetlight",
        "street lamp",
        "lamp",
        "light"
    ]


    traffic_keywords = [
        "traffic",
        "car",
        "vehicle",
        "automobile",
        "bus",
        "truck",
        "motorcycle"
    ]


    # -------------------------------------------------
    # Garbage
    # -------------------------------------------------

    if any(
        keyword in label
        for label in labels_lower
        for keyword in garbage_keywords
    ):

        return "Garbage"


    # -------------------------------------------------
    # Pothole
    # -------------------------------------------------

    if any(
        keyword in label
        for label in labels_lower
        for keyword in pothole_keywords
    ):

        return "Pothole"


    # -------------------------------------------------
    # Streetlight
    # -------------------------------------------------

    if any(
        keyword in label
        for label in labels_lower
        for keyword in streetlight_keywords
    ):

        return "Streetlight"


    # -------------------------------------------------
    # Traffic
    # -------------------------------------------------

    if any(
        keyword in label
        for label in labels_lower
        for keyword in traffic_keywords
    ):

        return "Traffic"


    return "Unknown"


# =================================================
# API RESPONSE
# =================================================

def response(
    status_code,
    body
):

    return {

        "statusCode":
            status_code,

        "headers": {

            "Content-Type":
                "application/json",

            "Access-Control-Allow-Origin":
                "*",

            "Access-Control-Allow-Headers":
                "Content-Type,Authorization",

            "Access-Control-Allow-Methods":
                "GET,POST,PATCH,OPTIONS"
        },

        "body":
            json.dumps(
                body,
                default=str
            )
    }

