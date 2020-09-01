/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
    initMemberListTable(this->memberNode);
    srand(time(NULL));
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	char* msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {

        size_t msgsize = sizeof(short) + sizeof(joinaddr->addr) + sizeof(long);

        msg = (char *) malloc(msgsize * sizeof(char));
        
        // create JOINREQ message: format of data is {struct Address myaddr}
        short type = JOINREQ;
        memcpy(msg, &type, sizeof(short));
        memcpy(msg + sizeof(short), memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy(msg + sizeof(short)+ sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));


#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    return -1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();


    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    //increase timestamp:
    memberNode->heartbeat++;
    updateSelfMemberListEntry();

    return;
}

void MP1Node::updateSelfMemberListEntry() {
    for (int i = 0; i < memberNode->memberList.size(); i++) {
        int id;
        short port;
        memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
        memcpy(&port, &memberNode->addr.addr[1], sizeof(short));
        if(memberNode->memberList[i].getid() == id && memberNode->memberList[i].getport() == port) {
            memberNode->memberList[i].setheartbeat(memberNode->heartbeat);
            memberNode->memberList[i].settimestamp(memberNode->heartbeat);
            return ;
        }
    }
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    short type;
    Address address;
    long heartbeat;

    memcpy(&type, data, sizeof(short));
    memcpy(&address.addr, data + sizeof(short), sizeof(address.addr));
    memcpy(&heartbeat, data + sizeof(short) + sizeof(address.addr), sizeof(long));

    if (type == JOINREQ) {
        handleJoin(address, heartbeat);
    }
    else if (type == JOINREP || type == GOSSIP) {
        if (type == JOINREP)
            memberNode->inGroup = true;
        updateMemberList(data, size);
    }
    return true;
}

void MP1Node::handleJoin(Address address, long heartbeat) {
    int id = 0;
    short port;
    memcpy(&id, &address.addr[0], sizeof(int));
    memcpy(&port, &address.addr[4], sizeof(short));
    MemberListEntry entry(id, port, heartbeat, memberNode->heartbeat);

    if (!isItInMembershipList(entry)) {
        memberNode->memberList.push_back(entry);
        log->logNodeAdd(&memberNode->addr, &address);
        memberNode->nnb ++;
     
        sendJoinReply(address);
    }
}

void MP1Node::sendJoinReply(Address addr) {
    sendMemberList(JOINREP, addr);
}

void MP1Node::sendMemberList(enum MsgTypes msgType, Address address) {

    size_t initSize = sizeof(short) + sizeof(address.addr) + sizeof(long);

    char* msg = (char *) malloc(initSize * sizeof(char));
    short type = msgType;
    long dummyHeartbeat = -1;

    memcpy(msg, &type, sizeof(short));
    memcpy(msg + sizeof(short), &memberNode->addr.addr, sizeof(address.addr));
    memcpy(msg + sizeof(short)+ sizeof(address.addr), &dummyHeartbeat, sizeof(long));
   
    size_t sizeOfMessage = initSize;
    for (int i = 0 ; i < memberNode->memberList.size(); i++) {
        if (memberNode->heartbeat - memberNode->memberList[i].gettimestamp() > TFAIL)
            continue;

        Address memberAddr;
        memcpy(&memberAddr.addr[0], &memberNode->memberList[i].id, sizeof(int));
        memcpy(&memberAddr.addr[4], &memberNode->memberList[i].port, sizeof(short));
        
        if (strcmp(memberAddr.addr, address.addr) != 0) {

            size_t newAddedSize = sizeof(memberAddr.addr) + sizeof(long);
            msg = (char*) realloc(msg, sizeOfMessage + newAddedSize);
            memcpy(msg+sizeOfMessage, memberAddr.addr, sizeof(memberAddr.addr));
            memcpy(msg+sizeOfMessage+sizeof(memberAddr.addr), &memberNode->memberList[i].heartbeat, sizeof(long));
            sizeOfMessage += newAddedSize;
            
        }
    }
    emulNet->ENsend(&memberNode->addr, &address, (char *)msg, sizeOfMessage);
    free(msg);
}

void printMemberList(vector<MemberListEntry> list) {
    cout << "-----------------------------" << endl;
    for (int i = 0; i < list.size(); i++) {
         cerr << "ID: " << list[i].getid() << " PORT: " << list[i].getport() << " HEART BEAT: " << list[i].getheartbeat() << " LOCAL: " << list[i].gettimestamp() << endl;
    }
    cout << "-----------------------------" << endl;

}

void MP1Node::updateMemberList(char* msg, int msgSize) {
    vector<MemberListEntry> msgMemberList = getMemberListFromMsg(msg, msgSize);

    for (int i = 0 ; i < msgMemberList.size(); i++) {
        if (!isItInMembershipList(msgMemberList[i])) {
            memberNode->memberList.push_back(msgMemberList[i]);
            memberNode->nnb++;
            Address address;
            memcpy(&address.addr[0], &msgMemberList[i].id, sizeof(int));
		    memcpy(&address.addr[4], &msgMemberList[i].port, sizeof(short));
            log->logNodeAdd(&memberNode->addr, &address);
        }
        else {
            int j = findMemberEntry(msgMemberList[i]);
            if (memberNode->memberList[j].getheartbeat() < msgMemberList[i].getheartbeat() 
                    &&  memberNode->heartbeat - memberNode->memberList[j].gettimestamp() <= TFAIL) {
                memberNode->memberList[j].setheartbeat(msgMemberList[i].getheartbeat());
                memberNode->memberList[j].settimestamp(memberNode->heartbeat);
            }
        }
    }
    
}

vector<MemberListEntry> MP1Node::getMemberListFromMsg(char* msg, int msgSize) {
    vector<MemberListEntry> memberList;

    size_t headerSize = sizeof(short) + sizeof(memberNode->addr.addr) + sizeof(long);
    
    for( size_t i = headerSize ; i < msgSize ; i += (6+sizeof(long)) ) {
        int id;
        short port;
        long heartbeat;
        memcpy(&id, msg+i, sizeof(int));
        memcpy(&port, msg+i+4, sizeof(short));
        memcpy(&heartbeat, msg+i+6, sizeof(long));
        MemberListEntry entry(id, port, heartbeat, memberNode->heartbeat);

       
        memberList.push_back(entry);
    }
    return memberList;
}


bool MP1Node::isItInMembershipList(MemberListEntry entry) {
   return findMemberEntry(entry) != -1;
}

int MP1Node::findMemberEntry(MemberListEntry entry) {
    for(int i = 0 ; i < memberNode->memberList.size(); i++) {
        if (memberNode->memberList[i].id == entry.id && memberNode->memberList[i].port == entry.port) 
            return i;
    }
    return -1;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    // Delete timeouted 
    int size = memberNode->memberList.size();
    for (int i = 0 ; i < size; i++) {
        if(memberNode->heartbeat - memberNode->memberList[i].gettimestamp() >= TREMOVE) {
            Address memberAddr;
            memcpy(&memberAddr.addr[0], &memberNode->memberList[i].id, sizeof(int));
            memcpy(&memberAddr.addr[4], &memberNode->memberList[i].port, sizeof(short));

            memberNode->memberList.erase(memberNode->memberList.begin() + i);
            memberNode->nnb--;
            size--;
            i--;

            log->logNodeRemove(&memberNode->addr, &memberAddr);          
  
        }
    }

    // Gossip
    vector<int> randomSelectedIndices;
    while(randomSelectedIndices.size() != GOSSIP_MAX && randomSelectedIndices.size() != memberNode->memberList.size()) {
        int number = rand() % memberNode->memberList.size();
        bool inList = false;
        for(int i = 0 ; i < randomSelectedIndices.size(); i++) {
            if(number == randomSelectedIndices[i]) {
                inList = true;
                break;
            }
        }
        if (!inList) {
            randomSelectedIndices.push_back(number);
        }
    }

    for (int i = 0 ; i < randomSelectedIndices.size(); i++) {
        MemberListEntry entry =  memberNode->memberList[randomSelectedIndices[i]];
        Address memberAddr;
        memcpy(&memberAddr.addr[0], &entry.id, sizeof(int));
        memcpy(&memberAddr.addr[4], &entry.port, sizeof(short));
        sendMemberList(GOSSIP, memberAddr);
    }

}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    
    int id;
    short port;
    memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
    memcpy(&port, &memberNode->addr.addr[1], sizeof(short));
    MemberListEntry selfEntry(id, port, memberNode->heartbeat, memberNode->heartbeat);
    memberNode->memberList.push_back(selfEntry);
    memberNode->myPos = memberNode->memberList.end();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
