#ifndef SMQTTSESSION_FILE
#define SMQTTSESSION_FILE

#include <string>
#include <vector>
#include "sMQTTMessage.h"

class sMQTTClient;

//! RAM-only MQTT session state for one client id.
//! A session is created for every accepted CONNECT. It survives disconnects
//! only when the client connected with CleanSession=0 (persistent==true);
//! otherwise it is discarded as soon as the connection drops.
class sMQTTSession
{
public:
	sMQTTSession(const std::string &id)
		: clientId(id), client(0), persistent(false), offlineSince(0), nextPacketId(1) {}

	std::string clientId;
	//! active connection, or 0 while the session is offline
	sMQTTClient *client;
	//! true if the session was created by a CleanSession=0 connect
	bool persistent;
	//! millis() when the client went offline (used for idle reaping)
	unsigned long offlineSince;

	//! stored subscriptions (topic filter + granted QoS)
	struct Sub
	{
		std::string filter;
		unsigned char qos;
	};
	std::vector<Sub> subs;

	//! QoS1/2 messages queued while the client was offline
	struct Msg
	{
		std::string topic;
		std::string payload;
		unsigned char qos;
	};
	std::vector<Msg> queue;

	//! receiver-side QoS2 packet ids awaiting PUBREL (dedup)
	std::vector<unsigned short> inQoS2;

	//! sender-side in-flight QoS1 messages awaiting PUBACK
	struct OutQoS1
	{
		unsigned short id;
		sMQTTMessage msg;
		unsigned long nextRetry;
		unsigned char retries;
	};
	std::vector<OutQoS1> outQoS1;

	//! sender-side in-flight QoS2 messages awaiting PUBREC/PUBCOMP
	enum { Q2WaitPubRec = 0, Q2WaitPubComp = 1 };
	struct OutQoS2
	{
		unsigned short id;
		std::string topic;
		std::string payload;
		bool retain;
		unsigned char state;
		unsigned long nextRetry;
		unsigned char retries;
	};
	std::vector<OutQoS2> outQoS2;

	//! next per-connection packet identifier
	unsigned short nextPacketId;
};

typedef std::vector<sMQTTSession*> sMQTTSessionList;
#endif
