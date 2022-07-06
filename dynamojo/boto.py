from boto3 import Session

Session = Session()
DYNAMOCLIENT = Session.client("dynamodb")
