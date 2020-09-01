/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address)
{
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
//  */
// MP2Node::~MP2Node() {
// 	// delete ht;
// 	// delete memberNode;
// }

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing()
{
	bool ringHasChanged = this->updateRingVectorUsingMemberLists();
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */

	if (ringHasChanged && !ht->isEmpty())
		stabilizationProtocol();
}

bool MP2Node::updateRingVectorUsingMemberLists()
{
	vector<Node> curMemList;
	curMemList = getMembershipList();

	sort(curMemList.begin(), curMemList.end());
	vector<Node> newRing;
	vector<Node> prevRing = ring;

	for (int i = 0; i < curMemList.size(); i++)
		newRing.emplace_back(curMemList[i]);

	ring = newRing;

	//compare the rings:
	if (prevRing.size() != newRing.size())
		return true;
	for (int i = 0; i < newRing.size(); i++) {
		if (!(*newRing[i].getAddress() == *prevRing[i].getAddress()))
			return true;
	}
	return false;


	// cerr << findNodes("salam")[0].getAddress()->getAddress() << endl;
	// cerr << hashFunction(memberNode->addr.addr) << ": ";
	// for (int i = 0 ; i < ring.size(); i++)
	// 	cerr <<ring[i].getHashCode() << "-";
	// cerr << endl;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList()
{
	unsigned int i;
	vector<Node> curMemList;
	for (i = 0; i < this->memberNode->memberList.size(); i++)
	{
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key)
{
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value)
{
	vector<Node> replicas = findNodes(key);
	g_transID++;
	for (int i = 0; i < replicas.size(); i++)
	{
		Message msg(g_transID, memberNode->addr, MessageType::CREATE, key, value, (ReplicaType)i);
		// preventing from going on network when the node is here!
		if(*replicas[i].getAddress() == memberNode->addr)
			handleCreateMsg(msg);
		else
			emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
	// Create always meets quorom
	log->logCreateSuccess(&memberNode->addr, true, g_transID, key, value);
}
/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key)
{
	vector<Node> replicas = findNodes(key);
	g_transID++;
	for (auto replica : replicas)
	{
		string l = "sending to " + replica.getAddress()->getAddress();
		Message msg(g_transID, memberNode->addr, MessageType::READ, key);
		emulNet->ENsend(&memberNode->addr, replica.getAddress(), msg.toString());
	}
	EntryState state;
	state.timestap = memberNode->heartbeat;
	state.key = key;
	state.type = READ;
	waitedJobs.emplace(g_transID, state);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value)
{
	vector<Node> replicas = findNodes(key);
	g_transID++;
	for (int i = 0; i < replicas.size(); i++)
	{
		Message msg(g_transID, memberNode->addr, MessageType::UPDATE, key, value, (ReplicaType)i);
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
	EntryState state;
	state.timestap = memberNode->heartbeat;
	state.key = key;
	state.type = UPDATE;
	state.value = value;
	waitedJobs.emplace(g_transID, state);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key)
{
	vector<Node> replicas = findNodes(key);
	g_transID++;
	for (auto replica : replicas)
	{
		Message msg(g_transID, memberNode->addr, MessageType::DELETE, key);
		emulNet->ENsend(&memberNode->addr, replica.getAddress(), msg.toString());
	}
	EntryState state;
	state.timestap = memberNode->heartbeat;
	state.key = key;
	state.type = DELETE;
	waitedJobs.emplace(g_transID, state);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica)
{
	Entry entry(value, this->memberNode->heartbeat, replica);
	return ht->create(key, entry.convertToString());
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key)
{
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica)
{
	Entry entry(value, this->memberNode->heartbeat, replica);
	return ht->update(key, entry.convertToString());
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key)
{
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char *data;
	int size;

	/*
	 * Declare your local variables here
	 */
	// dequeue all messages and handle them
	while (!memberNode->mp2q.empty())
	{
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message msg(message);
		dispatchMessages(msg);
	}

	checkQuorumAndTimeout();
	
}

void MP2Node::checkQuorumAndTimeout() {
	map<int, EntryState>::iterator it = waitedJobs.begin();
	while (it != waitedJobs.end())
	{
		int tid = it->first;
		EntryState state = it->second;
		if (state.done) {
			it++;
			continue;
		}
		if (state.recievedValues.size() >= 2)
		{
			if(state.type == READ) {
				Entry entry(state.recievedValues[0]);

				log->logReadSuccess(&memberNode->addr, true, tid, state.key, entry.value);
			}
			else if (state.type == UPDATE)
				log->logUpdateSuccess(&memberNode->addr, true, tid, state.key, state.value);
			else if (state.type == DELETE)
				log->logDeleteSuccess(&memberNode->addr, true, tid, state.key);

			waitedJobs[tid].done = true;
			// cerr << memberNode->heartbeat - state.timestap << endl;
		}
		else if (memberNode->heartbeat - state.timestap >= FAIL_TIMEOUT) {
			if(state.type == READ) 
				log->logReadFail(&memberNode->addr, true, tid, state.key);
			else if (state.type == UPDATE)
				log->logUpdateFail(&memberNode->addr, true, tid, state.key, state.value);
			else if (state.type == DELETE)
				log->logDeleteFail(&memberNode->addr, true, tid, state.key);	
			waitedJobs[tid].done = true;
		}
		it++;
	}
}
void MP2Node::handleCreateMsg(Message message)
{
	bool createWasSuccessfull = this->createKeyValue(message.key, message.value, message.replica);
	if (createWasSuccessfull)
		log->logCreateSuccess(&memberNode->addr, false, message.transID, message.key, message.value);
	else
		log->logCreateFail(&memberNode->addr, false, message.transID, message.key, message.value);
}

void MP2Node::handleUpdateMsg(Message message)
{
	bool updateWasSuccessfull = this->updateKeyValue(message.key, message.value, message.replica);
	if (updateWasSuccessfull)
	{
		log->logUpdateSuccess(&memberNode->addr, false, message.transID, message.key, message.value);
		Message msg(message.transID, memberNode->addr, MessageType::REPLY, message.key, message.value);
		emulNet->ENsend(&memberNode->addr, &message.fromAddr, msg.toString());
	}
	else
	{
		log->logUpdateFail(&memberNode->addr, false, message.transID, message.key, message.value);
	}
}

void MP2Node::handleReadMsg(Message message)
{
	string value = this->readKey(message.key);
	if (value != "")
	{
		log->logReadSuccess(&memberNode->addr, false, message.transID, message.key, value);
		Message msg(message.transID, memberNode->addr, MessageType::READREPLY, message.key, value);
		emulNet->ENsend(&memberNode->addr, &message.fromAddr, msg.toString());
	}
	else
	{
		log->logReadFail(&memberNode->addr, false, message.transID, message.key);
	}
}

void MP2Node::handleDeleteMsg(Message message)
{
	bool deleteWasSuccessfull = this->deletekey(message.key);
	if (deleteWasSuccessfull)
	{
		log->logDeleteSuccess(&memberNode->addr, false, message.transID, message.key);
		Message msg(message.transID, memberNode->addr, MessageType::REPLY, message.key);
		emulNet->ENsend(&memberNode->addr, &message.fromAddr, msg.toString());
	}
	else
	{
		log->logDeleteFail(&memberNode->addr, false, message.transID, message.key);
	}
}

void MP2Node::handleReplyMsg(Message message)
{
	map<int, EntryState>::iterator search;
	search = waitedJobs.find(message.transID);
	if (search != waitedJobs.end())
	{
		string val = message.type == REPLY ? "" : message.value;
		waitedJobs[message.transID].recievedValues.push_back(val);
	}
}

void MP2Node::dispatchMessages(Message message)
{
	switch (message.type)
	{
	case MessageType::CREATE:
		this->handleCreateMsg(message);
		break;
	case MessageType::UPDATE:
		this->handleUpdateMsg(message);
		break;
	case MessageType::READ:
		this->handleReadMsg(message);
		break;
	case MessageType::DELETE:
		this->handleDeleteMsg(message);
		break;
	case MessageType::REPLY:
	case MessageType::READREPLY:
		this->handleReplyMsg(message);
		break;
	default:
		break;
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key)
{
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3)
	{
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode())
		{
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else
		{
			// go through the ring until pos <= node
			for (int i = 1; i < ring.size(); i++)
			{
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode())
				{
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
					addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop()
{
	if (memberNode->bFailed)
	{
		return false;
	}
	else
	{
		return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size)
{
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol()
{
	map<string, string> hashTable = ht->hashTable;
	
	for (auto it = hashTable.begin(); it != hashTable.end(); it++) {
		vector<Node> replicas = findNodes(it->first);
		Entry entry(it->second);

		bool inReplicas = false;
		for (auto replica : replicas)
			if (*replica.getAddress() == memberNode->addr)
				inReplicas = true;

		if (!inReplicas) {
			ht->deleteKey(it->first);
		}
		// Create replicas again
		this->clientCreate(it->first, entry.value);
		// TODO: create replicas more selectively and efficiently!
	}
}
