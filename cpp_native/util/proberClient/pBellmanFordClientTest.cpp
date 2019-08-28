//
// Created by liqi on 19-7-17.
//

#include "BellmanFord/pBellmanFordClient.h"

#include <iostream>

int main(int argc, char *argv[])
{
    if(argc != 6)
    {
        std::cout << "Usage:" << std::endl << "./pBellmanFordClient vCount eCount numOfInitV nodeNo batchSize" << std::endl;
        std::cout << "An active server is required" <<std::endl;
        return 1;
    }

    int vCount = atoi(argv[1]);
    int eCount = atoi(argv[2]);
    int numOfInitV = atoi(argv[3]);
    int nodeNo = atoi(argv[4]);
    int batchSize = atoi(argv[5]);

    auto testUtilServer = pBellmanFordClient(vCount, eCount, numOfInitV, nodeNo, batchSize);

    int connected = testUtilServer.connectToServer();
    if(! connected)
    {
        std::cout << "Cannot connect to server" << std::endl;
        return 2;
    }

    testUtilServer.execute();
}
