//
// Created by liqi on 19-6-22.
//

#ifndef PREGELGPU_PBELLMANFORDCLIENT_H
#define PREGELGPU_PBELLMANFORDCLIENT_H

#include "../../../Graph_Algo/srv/UtilClient.h"

class pBellmanFordClient : UtilClient<double>{
public:
    pBellmanFordClient(int vCount, int eCount, int numOfInitV, int nodeNo = 0, int batch = 10000);
    ~pBellmanFordClient() = default;

    int connectToServer();
    void execute();

private:

    int* tempNode;
    bool* tempActive;
    double* tempData;


    UNIX_shm tempNode_shm;
    UNIX_shm tempActive_shm;
    UNIX_shm tempData_shm;

    UNIX_msg proberData_msg;
    UNIX_msg clientProber_msg;

    UNIX_msg init_msg;

};


#endif //PREGELGPU_PBELLMANFORDCLIENT_H
