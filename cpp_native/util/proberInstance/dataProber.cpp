//
// Created by liqi on 19-6-22.
//

#include <cstring>
#include <iostream>
#include <stdlib.h>
#include <srv/UtilClient.h>
#include "dataProber.h"

bool Unix_Message_Get(UNIX_msg controller, int controller_Macro, std::string &controlMark){

    char msgp[256];
    std::string cmd = std::string("");

    while(controller.recv(msgp, (controller_Macro << MSG_TYPE_OFFSET), 256) != -1){
        cmd = msgp;
        if(controlMark == cmd){
            return true;
        }
    }
    return false;
}

dataProber::dataProber(int pid, int vertexCount, int attrCount) {

    this->nodeNo = pid;
    this->vertexCount = vertexCount;
    this->attrCount = attrCount;

    this->tempData = nullptr;
    this->tempNode = nullptr;
    this->tempActive = nullptr;

    this->tempData_shm = UNIX_shm();
    this->tempNode_shm = UNIX_shm();
    this->tempActive_shm = UNIX_shm();

    this->clientProber_msg = UNIX_msg();
    this->proberData_msg = UNIX_msg();
    this->init_msg = UNIX_msg();

}

int dataProber::connectToClient() {

    int ret = 0;

    if(ret != -1) ret = this->tempActive_shm.fetch(((this->nodeNo << NODE_NUM_OFFSET) | (TEMPACTIVE_SHM << SHM_OFFSET)));
    if(ret != -1) ret = this->tempNode_shm.fetch(((this->nodeNo << NODE_NUM_OFFSET) | (TEMPNODE_SHM << SHM_OFFSET)));
    if(ret != -1) ret = this->tempData_shm.fetch(((this->nodeNo << NODE_NUM_OFFSET) | (TEMPDATA_SHM << SHM_OFFSET)));

    printf("%d :000 %d", ret, errno);

    if(ret != -1) ret = this->clientProber_msg.fetch(((this->nodeNo << NODE_NUM_OFFSET) | (PROCLI_MSG_TYPE << MSG_TYPE_OFFSET)));
    if(ret != -1) ret = this->proberData_msg.fetch(((this->nodeNo << NODE_NUM_OFFSET) | (PRODT_MSG_TYPE << MSG_TYPE_OFFSET)));
    if(ret != -1) ret = this->init_msg.fetch(((this->nodeNo << NODE_NUM_OFFSET) | (INIT_MSG_TYPE << MSG_TYPE_OFFSET)));

    if(ret != -1){
        this->tempData_shm.attach(0666);
        this->tempNode_shm.attach(0666);
        this->tempActive_shm.attach(0666);

        this->tempActive = (bool *)this->tempActive_shm.shmaddr;
        this->tempNode = (int *)this->tempNode_shm.shmaddr;
        this->tempData = (double *)this->tempData_shm.shmaddr;
    }

    return ret;

}

int dataProber::sendToClient(long *vertexNode, bool *vertexActive, double *vertexAttr) {

    if(this->vertexCount > 0){

        if(this->tempActive == nullptr) return -1;
        if(this->tempNode == nullptr) return -1;
        if(this->tempData == nullptr) return -1;

        memcpy(this->tempActive, vertexActive, vertexCount * sizeof(bool));
        memcpy(this->tempNode, vertexNode, vertexCount * sizeof(long));
        memcpy(this->tempData, vertexAttr, vertexCount * attrCount * sizeof(double));

        std::string tempVertex = std::to_string(vertexCount);
        this->proberData_msg.send(tempVertex.c_str(), (PRODT_MSG_TYPE << MSG_TYPE_OFFSET), 256);

        std::string cmd = std::string("finished");

        bool status = Unix_Message_Get(this->clientProber_msg, PROCLI_MSG_TYPE, cmd);

        if(status) return 1;
    }

    else return -1;

}

int dataProber::resultGenerate() {

    this->proberData_msg.send("end", (PRODT_MSG_TYPE << MSG_TYPE_OFFSET), 256);

    std::string cmd = std::string("calculation completed");

    bool status = Unix_Message_Get(this->clientProber_msg, PROCLI_MSG_TYPE, cmd);

    if(status)return 1;

    else return -1;
}

void dataProber::disconnect() {

    this->tempData_shm.detach();
    this->tempNode_shm.detach();
    this->tempActive_shm.detach();

    this->tempData = nullptr;
    this->tempNode = nullptr;
    this->tempActive = nullptr;
}




