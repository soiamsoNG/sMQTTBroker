#ifndef SMQTTCLIENT_FILE
#define SMQTTCLIENT_FILE

#include "sMQTTMessage.h"

class sMQTTBroker;
class sMQTTSession;

#define sMQTTUserNameFlag 0x80
#define sMQTTPasswordFlag 0x40
#define sMQTTWillRetainFlag 0x20
#define sMQTTWillQoSFlag 0x18
#define sMQTTWillFlag 0x4
#define sMQTTCleanSessionFlag 0x2

#define sMQTTConnReturnAccepted 0x0
#define sMQTTConnReturnUnacceptableProtocolVersion 0x1
#define sMQTTConnReturnIdentifierRejected 0x2
#define sMQTTConnReturnServerUnavailable 0x3
#define sMQTTConnReturnBadUsernameOrPassword 0x4

#define sMQTTQoS1RetryInterval 3000
#define sMQTTQoS2RetryInterval 3000

//!\brief Main Client class
class sMQTTClient
{
public:
	sMQTTClient(sMQTTBroker *parent, TCPClient &client);
	~sMQTTClient();

	void update();
	
	//! check connection
	bool isConnected();
	void write(const char* buf, size_t length);

	//! send a PUBLISH to this client, QoS1 messages are tracked for retransmission
	void sendPublish(const std::string &topic, const std::string &payload, unsigned char qos, bool retain=false);
	//! set QoS1 retransmission interval (ms)
	void setQoS1RetryInterval(unsigned long ms) { _qos1RetryMs = ms; };
	//! set QoS2 retransmission interval (ms)
	void setQoS2RetryInterval(unsigned long ms) { _qos2RetryMs = ms; };

	//! get client id
	const std::string &getClientId() {
		return clientId;
	};
private:
	friend class sMQTTBroker;

	void processMessage();
	void updateLiveStatus();
	unsigned short allocPacketId();

	char mqtt_flags;
	bool mqtt_connected;
	std::string clientId;
	unsigned short keepAlive;
	unsigned long aliveMillis;

	unsigned long _qos1RetryMs;
	unsigned long _qos2RetryMs;

	//! session state (subscriptions, offline queue, in-flight QoS1/2)
	sMQTTSession *_session;

	sMQTTBroker *_parent;
	TCPClient _client;
	sMQTTMessage message;
};

typedef std::vector<sMQTTClient*> sMQTTClientList;
#endif