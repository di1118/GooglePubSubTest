using Google.Cloud.PubSub.V1;
using System;
using System.Linq;
using Google.Protobuf;

namespace GooglePubSubTest
{
	class Program
	{
		static int  Main(string[] args)
		{
			// Use a switch statement to do the math.
			Console.WriteLine("Pub or SubPull: ");
			switch (Console.ReadLine())
			{
				case "Pub":
					Console.WriteLine("Message to Push: ");
					string mess = Console.ReadLine();
					// First create a topic.
					PublisherServiceApiClient publisher = PublisherServiceApiClient.Create();
					TopicName topicName = new TopicName("pubSubfirst", "order_topic");
					//publisher.CreateTopic(topicName);
					// // Publish a message to the topic.
					PubsubMessage message = new PubsubMessage
					{
						// The data is any arbitrary ByteString. Here, we're using text.
						Data = ByteString.CopyFromUtf8(mess),
						// The attributes provide metadata in a string-to-string dictionary.
						Attributes =
						{
							{"description", "Simple text message"}
						}
					};
					publisher.Publish(topicName, new[] {message});
					break;
				case "SubPull":
					// Subscribe to the topic.
					SubscriberServiceApiClient subscriber = SubscriberServiceApiClient.Create();
					SubscriptionName subscriptionName = new SubscriptionName("PubSubFirst", "orderTopicSubscr");
					//subscriber.CreateSubscription(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);
					subscriber.GetSubscription(subscriptionName, null);

					// Pull messages from the subscription. We're returning immediately, whether or not there
					// are messages; in other cases you'll want to allow the call to wait until a message arrives.
					PullResponse response = subscriber.Pull(subscriptionName, returnImmediately: true, maxMessages: 10);
					Console.WriteLine("Pull Response");
					var msgnumber = response.ReceivedMessages.Count;
					Console.WriteLine(msgnumber);
					foreach (ReceivedMessage received in response.ReceivedMessages)
					{
						Console.WriteLine("Message:");
						PubsubMessage msg = received.Message;
						Console.WriteLine(
							$"Received message {msg.MessageId} published at {msg.PublishTime.ToDateTime()}");
						Console.WriteLine($"Text: '{msg.Data.ToStringUtf8()}'");

					}

					// Acknowledge that we've received the messages. If we don't do this within 60 seconds (as specified
					// when we created the subscription) we'll receive the messages again when we next pull.
					if (response.ReceivedMessages.Count > 0)
					{
						subscriber.Acknowledge(subscriptionName, response.ReceivedMessages.Select(m => m.AckId));
					}

					break;
			}



			// Tidy up by deleting the subscription and the topic.
			//subscriber.DeleteSubscription(subscriptionName);
			//publisher.DeleteTopic(topicName);
			return 0;

		}
	}
}
