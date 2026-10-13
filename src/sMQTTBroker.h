/*!
Main class
*/
#ifndef SMQTTBROKER_FILE
#define SMQTTBROKER_FILE

#include"sMQTTplatform.h"
#include"sMQTTClient.h"
#include"sMQTTTopic.h"
#include"sMQTTSession.h"
#include"sMQTTEvent.h"

//! idle timeout before a persistent (CleanSession=0) session is discarded
#define sMQTTSessionTimeoutMs 300000
//! maximum number of messages queued per offline session
#define sMQTTSessionQueueLimit 128

class sMQTTBroker
{
public:
	/*! setup broker \param port set listen port \param checkWifiConnection enable/disable notify wifi connection in onEvent*/
	bool init(unsigned short port, bool checkWifiConnection=false);
	/*! call in loop function*/
	void update();
	/*! publish message
		\param topic name of topic
		\param payload
		\param qos
		\param retain
	*/
	void publish(const std::string &topic, const std::string &payload,unsigned char qos=0,bool retain=false);
	//! restart WIFI server
	void restart();
	//! receive event from broker \param event
	virtual bool onEvent(sMQTTEvent *event) = 0;

	//! receive retained topic count
	unsigned long getRetainedTopicCount();
	//! receive topic name by index \param index index of topic
	std::string getRetaiedTopicName(unsigned long index);
private:
	// inner function
	void publish(sMQTTClient *client, sMQTTTopic *topic, sMQTTMessage *msg);

	bool subscribe(sMQTTClient *client, const char *topic, unsigned char qos);
	void unsubscribe(sMQTTClient *client, const char *topic);

	bool isTopicValidName(const char *filter);
	void updateRetainedTopic(sMQTTTopic *topic);

	bool isClientConnected(sMQTTClient *client);

	void findRetainTopic(sMQTTTopic *topic, sMQTTClient *client, unsigned char maxQoS);

	// session handling
	unsigned char attachSession(sMQTTClient *client, const std::string &clientId, bool clean);
	void detachSession(sMQTTClient *client);
	void reapSessions();
	void resumeSubscription(sMQTTClient *client, const char *topic);
	void enqueueOffline(const std::string &topic, const std::string &payload, unsigned char qos);
	void flushSession(sMQTTSession *session);
	sMQTTSession *findSession(const std::string &clientId);
	void removeSession(sMQTTSession *session);

	TCPServer *_server;
	sMQTTClientList clients;
	sMQTTTopicList subscribes, retains;
	sMQTTSessionList sessions;
	bool isCheckWifiConnection;
	friend class sMQTTClient;
};

class sMQTTBrokerWithoutEvent:public sMQTTBroker
{
public:
	bool onEvent(sMQTTEvent *event) { return true; }
};
#endif