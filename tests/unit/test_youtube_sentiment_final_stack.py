import aws_cdk as core
import aws_cdk.assertions as assertions

from youtube_sentiment_final.youtube_sentiment_final_stack import YoutubeSentimentFinalStack

# example tests. To run these tests, uncomment this file along with the example
# resource in youtube_sentiment_final/youtube_sentiment_final_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = YoutubeSentimentFinalStack(app, "youtube-sentiment-final")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
