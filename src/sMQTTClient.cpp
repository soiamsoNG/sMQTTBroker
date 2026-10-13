#include "sMQTTBroker.h"
#include <algorithm>

sMQTTClient::sMQTTClient(sMQTTBroker *parent, TCPClient &client):mqtt_connected(false), _parent(parent)
{
	_client = client;
	keepAlive = 25;
	_nextPacketId = 1;
	_qos1RetryMs = sMQTTQoS1RetryInterval;
	_qos2RetryMs = sMQTTQoS2RetryInterval;
	updateLiveStatus();
};
sMQTTClient::~sMQTTClient()
{
	//SMQTT_LOGD("free _client");
	//delete _client;
};
void sMQTTClient::update()
{
	while (_client.available()>0)
	{
		message.incoming(_client.read());
		if (message.type())
		{
			processMessage();
			message.reset();
			break;
		}
	}
	unsigned long currentMillis;
#if defined(ESP8266) || defined(ESP32)
	currentMillis = millis();
#endif
	if (keepAlive != 0 && aliveMillis < currentMillis)
	{
		SMQTT_LOGD("aliveMillis(%lu) < currentMillis(%lu)", aliveMillis, currentMillis);
		_client.stop();
	}
	// QoS1 retransmission (DUP=1, same packet identifier)
	for (auto &p : _outQoS1)
	{
		if (currentMillis >= p.nextRetry)
		{
			p.msg.setDup();
			p.msg.sendTo(this, false);
			p.nextRetry = currentMillis + _qos1RetryMs;
			p.retries++;
		}
	}
	// QoS2 retransmission (PUBLISH with DUP=1 until PUBREC, then PUBREL until PUBCOMP)
	for (auto &p : _outQoS2)
	{
		if (currentMillis >= p.nextRetry)
		{
			if (p.state == Q2WaitPubRec)
			{
				sMQTTMessage msg(sMQTTMessage::Type::Publish, (2 << 1) | (p.retain ? 1 : 0));
				msg.setDup();
				msg.add(p.topic.c_str(), p.topic.size());
				msg.add(p.id >> 8);
				msg.add(p.id);
				msg.add(p.payload.c_str(), p.payload.size(), false);
				msg.sendTo(this);
			}
			else
			{
				sMQTTMessage msg(sMQTTMessage::Type::PubRel, 0x02);
				msg.add(p.id >> 8);
				msg.add(p.id);
				msg.sendTo(this);
			}
			p.nextRetry = currentMillis + _qos2RetryMs;
			p.retries++;
		}
	}
	//else
	//	SMQTT_LOGD("time %d", aliveMillis - currentMillis);
};
bool sMQTTClient::isConnected()
{
	return _client.connected();
};
void sMQTTClient::write(const char* buf, size_t length)
{
	_client.write(buf, length);
}
unsigned short sMQTTClient::allocPacketId()
{
	for (;;)
	{
		if (++_nextPacketId == 0)
			_nextPacketId = 1;
		bool used = false;
		for (auto &p : _outQoS1)
			if (p.id == _nextPacketId)
			{
				used = true;
				break;
			}
		for (auto &p : _outQoS2)
			if (p.id == _nextPacketId)
			{
				used = true;
				break;
			}
		if (!used)
			return _nextPacketId;
	}
}
void sMQTTClient::sendPublish(const std::string &topic, const std::string &payload, unsigned char qos, bool retain)
{
	unsigned short id = 0;
	sMQTTMessage msg(sMQTTMessage::Type::Publish, (qos << 1) | (retain ? 1 : 0));
	msg.add(topic.c_str(), topic.size());
	if (qos)
	{
		id = allocPacketId();
		msg.add(id >> 8);
		msg.add(id);
	}
	msg.add(payload.c_str(), payload.size(), false);
	msg.sendTo(this);
	if (qos == 1)
	{
		sMQTTOutPacket p;
		p.id = id;
		p.msg = msg;
		p.nextRetry = millis() + _qos1RetryMs;
		p.retries = 0;
		_outQoS1.push_back(p);
	}
	else if (qos == 2)
	{
		sMQTTOutQoS2 p;
		p.id = id;
		p.topic = topic;
		p.payload = payload;
		p.retain = retain;
		p.state = Q2WaitPubRec;
		p.nextRetry = millis() + _qos2RetryMs;
		p.retries = 0;
		_outQoS2.push_back(p);
	}
}
void sMQTTClient::processMessage()
{
	if (message.type() <= sMQTTMessage::Type::Disconnect)
	{
		SMQTT_LOGD("message type:%s(0x%x)", debugMessageType[message.type() / 0x10], message.type());
	}

	const char *header = message.getVHeader();
	switch (message.type())
	{
	case sMQTTMessage::Type::Connect:
		{
			if (mqtt_connected)
			{
				_client.stop();
				break;
			}
			unsigned char status = 0;
			if (strncmp("MQTT", header + 2, 4))
			{
				//TODO: close connection
			}
			if (header[6] != 0x04)
			{
				status = sMQTTConnReturnUnacceptableProtocolVersion;
				// Level 3.1.1
			}
			else
			{
				unsigned short len;
				mqtt_flags = header[7];
				keepAlive = (header[8] << 8) | header[9];

				const char *payload = &header[10];
				message.getString(payload, len);
				clientId = std::string(payload,len);
				payload += len;

				SMQTT_LOGD("message clientId:%s", clientId.c_str());
				SMQTT_LOGD("message keepTime:%d", keepAlive);

				if (mqtt_flags&sMQTTWillFlag)
				{
					//topic
					message.getString(payload, len);
					//willTopic = std::string(payload, len);
					payload += len;
					//message
					message.getString(payload, len);
					//willMessage = std::string(payload, len);
					payload += len;
				}
				std::string username;
				if (mqtt_flags&sMQTTUserNameFlag)
				{
					message.getString(payload, len);

					username = std::string(payload, len);
					SMQTT_LOGD("message user:%s", username.c_str());

					payload += len;
				}
				std::string password;
				if (mqtt_flags&sMQTTPasswordFlag)
				{
					message.getString(payload, len);
					password = std::string(payload, len);
					SMQTT_LOGD("message password:%s", password.c_str());
					payload += len;
				}

				if (_parent->isClientConnected(this) == false)
				{
					sMQTTNewClientEvent event(this, username, password);
					if(_parent->onEvent(&event)==false)
						status = sMQTTConnReturnBadUsernameOrPassword;
				}
				else
					status = sMQTTConnReturnIdentifierRejected;
			}

			sMQTTMessage msg(sMQTTMessage::Type::ConnAck);
			msg.add(0);	// Session present (not implemented)
			msg.add(status); // Connection accepted
			msg.sendTo(this);

			if (status)
				_client.stop();
			else
				mqtt_connected = true;
		}
		break;
	case sMQTTMessage::Type::Publish:
		{
			unsigned char qos = message.QoS();

			unsigned short len;
			const char *payload = header;
			message.getString(payload, len);

			const char *topicName = payload;
			std::string _topicName(topicName, len);
			payload += len;

			char packeteIdent[2]={0};
			if (qos)
			{
				packeteIdent[0] = payload[0];
				packeteIdent[1] = payload[1];
				payload += 2;
			}
			len = message.end() - payload;
			std::string _payload(payload, len);

			unsigned short pid = ((unsigned char)packeteIdent[0] << 8) | (unsigned char)packeteIdent[1];

			// QoS2 receiver-side dedup [MQTT-4.3.3-2]: a re-delivered PUBLISH whose
			// packet id was already acknowledged only triggers another PUBREC and is
			// not forwarded to subscribers a second time.
			if (qos == 2 && std::find(_inQoS2.begin(), _inQoS2.end(), pid) != _inQoS2.end())
			{
				sMQTTMessage msg(sMQTTMessage::Type::PubRec);
				msg.add(packeteIdent[0]);
				msg.add(packeteIdent[1]);
				msg.sendTo(this);
				break;
			}

			sMQTTTopic topic(_topicName,_payload, qos);

			if (message.isRetained())
				_parent->updateRetainedTopic(&topic);

			switch (qos)
			{
			case 1:
				{
					sMQTTMessage msg(sMQTTMessage::Type::PubAck);
					msg.add(packeteIdent[0]);
					msg.add(packeteIdent[1]);
					msg.sendTo(this);
				}
				break;
			case 2:
				{
					_inQoS2.push_back(pid);
					sMQTTMessage msg(sMQTTMessage::Type::PubRec);
					msg.add(packeteIdent[0]);
					msg.add(packeteIdent[1]);
					msg.sendTo(this);
				}
				break;
			}

			_parent->publish(this,&topic, &message);
		}
		break;
	case sMQTTMessage::Type::PubAck:
		{
			unsigned short id = (header[0] << 8) | header[1];
			for (std::vector<sMQTTOutPacket>::iterator it = _outQoS1.begin(); it != _outQoS1.end(); it++)
			{
				if (it->id == id)
				{
					_outQoS1.erase(it);
					break;
				}
			}
		}
		break;
	case sMQTTMessage::Type::PubRec:
		{
			const char *payload = header;
			unsigned short id = ((unsigned char)payload[0] << 8) | (unsigned char)payload[1];
			// sender side: PUBREC received, now wait for PUBCOMP; stop re-sending PUBLISH
			for (auto &p : _outQoS2)
			{
				if (p.id == id)
				{
					if (p.state == Q2WaitPubRec)
					{
						p.state = Q2WaitPubComp;
						p.nextRetry = millis() + _qos2RetryMs;
					}
					break;
				}
			}
			sMQTTMessage msg(sMQTTMessage::Type::PubRel, 0x02);
			msg.add(payload[0]);
			msg.add(payload[1]);
			msg.sendTo(this);
		}
		break;
	case sMQTTMessage::Type::PubRel:
		{
			const char *payload = header;
			unsigned short id = ((unsigned char)payload[0] << 8) | (unsigned char)payload[1];
			// receiver side: handshake complete, drop dedup state
			for (std::vector<unsigned short>::iterator it = _inQoS2.begin(); it != _inQoS2.end(); it++)
			{
				if (*it == id)
				{
					_inQoS2.erase(it);
					break;
				}
			}
			sMQTTMessage msg(sMQTTMessage::Type::PubComp);
			msg.add(payload[0]);
			msg.add(payload[1]);
			msg.sendTo(this);
		}
		break;
	case sMQTTMessage::Type::PubComp:
		{
			const char *payload = header;
			unsigned short id = ((unsigned char)payload[0] << 8) | (unsigned char)payload[1];
			// sender side: handshake complete, stop retransmission
			for (std::vector<sMQTTOutQoS2>::iterator it = _outQoS2.begin(); it != _outQoS2.end(); it++)
			{
				if (it->id == id)
				{
					_outQoS2.erase(it);
					break;
				}
			}
		}
		break;
	case sMQTTMessage::Type::Subscribe:
		{
#if ARDUHAL_LOG_LEVEL >= ARDUHAL_LOG_LEVEL_DEBUG
			unsigned short msg_id = (header[0] << 8) | header[1];
			SMQTT_LOGD("message id:%d", msg_id);
#endif
			const char *payload = header + 2;
			std::vector<char> qoss;
			while (payload < message.end())
			{
				unsigned short len;
				message.getString(payload, len);	// Topic

				std::string topic(payload, len);
				SMQTT_LOGD("message topic:%s", topic.c_str());
				payload += len;
				unsigned char qos = *payload++;

				if (_parent->subscribe(this, topic.c_str(), qos) == false)
				{
					SMQTT_LOGD("subscribe failed");
					qos = 0x80;
				}
				qoss.push_back(qos);
			}
			sMQTTMessage msg(sMQTTMessage::Type::SubAck);
			msg.add(header[0]);
			msg.add(header[1]);
			for (int i = 0; i<qoss.size(); i++)
				msg.add(qoss[i]);
			msg.sendTo(this);
		}
		break;
	case sMQTTMessage::Type::UnSubscribe:
		{
			//unsigned short msg_id = (header[0] << 8) | header[1];
			//SMQTT_LOGD("message id:%d", msg_id);
			const char *payload = header + 2;
			while (payload < message.end())
			{
				unsigned short len;
				message.getString(payload, len);	// Topic

				//SMQTT_LOGD("message topic:%s", std::string(payload, len).c_str());
				_parent->unsubscribe(this, std::string(payload, len).c_str());

				payload += len;
				//unsigned char qos = *payload++;
			}
			sMQTTMessage msg(sMQTTMessage::Type::UnSuback);
			msg.add(header[0]);
			msg.add(header[1]);
			msg.sendTo(this);
		}
		break;
	case sMQTTMessage::Type::Disconnect:
		{
			mqtt_connected = false;
			_client.stop();
		}
		break;
	case sMQTTMessage::Type::PingReq:
		{
			sMQTTMessage msg(sMQTTMessage::Type::PingResp);
			msg.sendTo(this);
		}
		break;
	default:
		{
			SMQTT_LOGD("unknown message %d", message.type());
			mqtt_connected = false;
			_client.stop();
		}
		break;
	}
	updateLiveStatus();
};
void sMQTTClient::updateLiveStatus()
{
	if (keepAlive)
#if defined(ESP8266) || defined(ESP32)
		//aliveMillis = (keepAlive*1.5) * 1000 + millis();
		aliveMillis = keepAlive*1500 + millis();
#else
		aliveMillis = 0;
#endif
	else
		aliveMillis = 0;
};