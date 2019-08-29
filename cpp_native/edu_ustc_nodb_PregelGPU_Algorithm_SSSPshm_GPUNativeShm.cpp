#include "edu_ustc_nodb_PregelGPU_Algorithm_SSSPshm_GPUNativeShm.h"

#include "util/JNIPlugin.h"

#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <zconf.h>

using namespace std;

template <typename T>
int openShm(const string &filename, int counter, T* &Arr)
{

    int counterSize = counter * sizeof(T);

    int chk = shm_open(filename.c_str(), O_RDWR, 0664);

    if(chk == -1) {
        cout<<errno<<endl;
        return -1;
    }

    void* ptr = mmap(nullptr, counterSize, PROT_READ | PROT_WRITE, MAP_SHARED, chk, 0);

    Arr = (T* )ptr;

    return 0;
}

template <typename T>
int writeShm(const string &filename, vector<T> &Arr)
{

    int counterSize = Arr.size() * sizeof(T);
    T* ArrAddress = &Arr[0];

    int chk = shm_open(filename.c_str(), O_RDWR | O_CREAT, 0664);

    if(chk == -1) {
        cout<<errno<<endl;
        return -1;
    }

    void* ptr = mmap(nullptr, counterSize, PROT_READ | PROT_WRITE, MAP_SHARED, chk, 0);

    int ft = ftruncate(chk, counterSize);
    if(ft == -1) {
        cout<<errno<<endl;
        return -1;
    }

    memcpy((T* )ptr, ArrAddress, counterSize);

    return 0;
}

// Init the edge and markID

JNIEXPORT jboolean JNICALL Java_edu_ustc_nodb_PregelGPU_Algorithm_SSSPshm_GPUNativeShm_nativeEnvEdgeInit
(JNIEnv * env, jobject superClass,
        jlongArray jFilteredVertex,
        jlong vertexSum, jlongArray jEdgeSrc, jlongArray jEdgeDst, jdoubleArray jEdgeAttr,
        jobject markId, jint pid){

    jclass c_Long = env->FindClass("java/lang/Long");
    jmethodID longValue = env->GetMethodID(c_Long, "longValue", "()J");

    jclass c_ArrayList = env->FindClass("java/util/ArrayList");
    jmethodID id_ArrayList_size = env->GetMethodID(c_ArrayList, "size", "()I");
    jmethodID id_ArrayList_get = env->GetMethodID(c_ArrayList, "get", "(I)Ljava/lang/Object;");

    //---------Entity---------

    int lenMarkID = env->CallIntMethod(markId, id_ArrayList_size);
    int lenEdge = env->GetArrayLength(jEdgeSrc);
    int lenFiltered = env->GetArrayLength(jFilteredVertex);

    int vertexAllSum = static_cast<int>(vertexSum);
    int partitionID = static_cast<int>(pid);

    //Init the Graph with blank vertices

    vector<Vertex> vertices = vector<Vertex>();
    double *vValues = new double [vertexAllSum * lenMarkID];
    bool* filteredV = new bool [vertexAllSum];

    vector<Edge> edges = vector<Edge>();

    for(int i = 0; i < vertexAllSum; i++){
        filteredV[i] = false;
        vertices.emplace_back(Vertex(i, false, INVALID_INITV_INDEX));
        for(int j = 0; j < lenMarkID; j++){
            vValues[i * lenMarkID + j] = (INT32_MAX >> 1);
        }
    }

    // fill markID, which stored the landmark

    int *initVSet = new int [lenMarkID];

    for(int i = 0; i < lenMarkID; i++){

        jobject start = env->CallObjectMethod(markId, id_ArrayList_get, i);
        jlong jMarkIDUnit = env->CallLongMethod(start, longValue);
        vertices.at(jMarkIDUnit).initVIndex = i;
        initVSet[i] = jMarkIDUnit;

        env->DeleteLocalRef(start);
    }

    // Init the Graph with existed edges
    jboolean isCopy = false;
    long* FilteredVertexTemp = env->GetLongArrayElements(jFilteredVertex, &isCopy);

    long* EdgeSrcTemp = env->GetLongArrayElements(jEdgeSrc, &isCopy);
    long* EdgeDstTemp = env->GetLongArrayElements(jEdgeDst, &isCopy);
    double * EdgeAttrTemp = env->GetDoubleArrayElements(jEdgeAttr, &isCopy);

    for(int i = 0; i < lenFiltered; i++){
        filteredV[FilteredVertexTemp[i]] = true;
    }

    for (int i = 0; i < lenEdge; i++) {

        int jSrcId_get = EdgeSrcTemp[i];
        //jlong SrcId_get = env->CallLongMethod(jSrcId, longValue);
        int jDstId_get = EdgeDstTemp[i];
        //jlong DstId_get = env->CallLongMethod(jDstId, longValue);
        double jAttr_get = EdgeAttrTemp[i];
        //jdouble Attr_get = env->CallDoubleMethod(jAttr, doubleValue);

        edges.emplace_back(Edge(jSrcId_get, jDstId_get, jAttr_get));

    }

    env->ReleaseLongArrayElements(jEdgeSrc, EdgeSrcTemp, 0);
    env->ReleaseLongArrayElements(jEdgeDst, EdgeDstTemp, 0);
    env->ReleaseDoubleArrayElements(jEdgeAttr, EdgeAttrTemp, 0);

    initProber detector = initProber(partitionID);
    bool status = detector.run();
    if(! status) return false;

    UtilClient<double, double> execute = UtilClient<double, double>(vertexAllSum, lenEdge, lenMarkID, partitionID);

    int chk = 0;

    chk = execute.connect();
    if (chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
        return false;
    }

    chk = execute.transfer(vValues, &vertices[0], &edges[0], initVSet, filteredV, lenFiltered);

    if(chk == -1){
        throwIllegalArgument(env, "Cannot transfer with server correctly");
        return false;
    }

    execute.disconnect();

    return true;
}


JNIEXPORT jint JNICALL Java_edu_ustc_nodb_PregelGPU_Algorithm_SSSPshm_GPUNativeShm_nativeStepMsgExecute
(JNIEnv * env, jobject superClass,
        jlong vertexSum, jobject shmWriter, jobject shmReader,
        jint vertexCount, jint edgeCount, jint markIdLen, jint pid){

    jclass writerClass = env->GetObjectClass(shmWriter);
    jmethodID getWriterName = env->GetMethodID(writerClass, "getNameByUnder", "(I)Ljava/lang/String;");
    jmethodID getWriterSize = env->GetMethodID(writerClass, "getSizeByUnder", "(I)I");

    jclass readerClass = env->GetObjectClass(shmReader);
    jmethodID addReaderName = env->GetMethodID(readerClass, "addName", "(Ljava/lang/String;I)Z");

    //---------Debug tools---------

    // jclass n_sztool = env->FindClass("edu/ustc/nodb/matrix/SizesTool");
    // jmethodID id_getSize = env->GetStaticMethodID(n_sztool, "getObjectSize", "(Ljava/lang/Object;)J");

    //-----------------------------

    auto startTimeAll = std::chrono::high_resolution_clock::now();
    auto startTimeA = std::chrono::high_resolution_clock::now();

    int vertexAllSum = static_cast<int>(vertexSum);
    int partitionID = static_cast<int>(pid);
    int lenMarkID = static_cast<int>(markIdLen);
    int lenEdge = static_cast<int>(edgeCount);
    int lenVertex = static_cast<int>(vertexCount);

    UtilClient<double, double> execute = UtilClient<double, double>(vertexAllSum, lenEdge, lenMarkID, partitionID);

    int chk = 0;

    chk = execute.connect();
    if (chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
    }

    // Init the vertices

    vector<Vertex> vertices = vector<Vertex>();
    double *vValues = new double [vertexAllSum * lenMarkID];

    for(int i = 0; i < vertexAllSum; i++){
        vertices.emplace_back(Vertex(i, false, INVALID_INITV_INDEX));
        for(int j = 0; j < lenMarkID; j++){
            vValues[i * lenMarkID + j] = (INT32_MAX >> 1);
        }
    }

    for(jint i = 0; i < lenMarkID; i++){
        vertices.at(execute.initVSet[i]).initVIndex = i;
    }

    // fill vertices attributes
    long* VertexIDTemp;
    bool * VertexActiveTemp;
    double* VertexAttrTemp;

    string VertexIDTempName = jString2String(env, (jstring)env->CallObjectMethod(shmWriter, getWriterName, 0));
    string VertexActiveTempName = jString2String(env, (jstring)env->CallObjectMethod(shmWriter, getWriterName, 1));
    string VertexAttrTempName = jString2String(env, (jstring)env->CallObjectMethod(shmWriter, getWriterName, 2));

    int sizeID = env->CallIntMethod(shmWriter, getWriterSize, 0);
    int sizeActive = env->CallIntMethod(shmWriter, getWriterSize, 1);
    int sizeAttr = env->CallIntMethod(shmWriter, getWriterSize, 2);

    chk = openShm(VertexIDTempName, sizeID, VertexIDTemp);
    if (chk == -1){
        throwIllegalArgument(env, "Shared memory corruption");
    }

    chk = openShm(VertexActiveTempName, sizeActive, VertexActiveTemp);
    if (chk == -1){
        throwIllegalArgument(env, "Shared memory corruption");
    }

    chk = openShm(VertexAttrTempName, sizeAttr, VertexAttrTemp);
    if (chk == -1){
        throwIllegalArgument(env, "Shared memory corruption");
    }

    for (int i = 0; i < lenVertex; i++) {

        // jlong sig = env->CallStaticLongMethod(n_sztool, id_getSize, vertexLL);

        long jVertexId_get = VertexIDTemp[i];

        for(int j = 0; j < lenMarkID; j++){
            double jDis = VertexAttrTemp[i * lenMarkID + j] ;
            int index = vertices.at(execute.initVSet[j]).initVIndex;
            if(index != INVALID_INITV_INDEX) vValues[jVertexId_get * lenMarkID + index] = jDis;
        }

        bool jVertexActive_get = VertexActiveTemp[i];

        vertices.at(jVertexId_get).isActive = jVertexActive_get;

    }

/*
    // test for multithreading environment

    std::string outputT = std::string();

    for(int i = 0;i < vertexAllSum; i++){
        outputT += to_string(i) + ": {";
        for(int j = 0;j < lenMarkID; j++){
            outputT += " [" + to_string(execute.initVSet[j])+": "+to_string(execute.mValues[i * lenMarkID + j]) + "] ";
        }
        outputT += "}";
    }

    cout<<outputT<<endl;

    outputT.clear();
    // test end
*/
    std::chrono::nanoseconds durationA = std::chrono::high_resolution_clock::now() - startTimeA;
    auto startTime = std::chrono::high_resolution_clock::now();

    // execute sssp using GPU in server-client mode

    chk = execute.update(vValues, &vertices[0]);

    if(chk == -1){
        throwIllegalArgument(env, "Cannot update with server correctly");
    }

    execute.request();

    std::chrono::nanoseconds duration = std::chrono::high_resolution_clock::now() - startTime;

    auto startTimeB = std::chrono::high_resolution_clock::now();

    vector<long> cPlusReturnId = vector<long>();
    vector<double> cPlusReturnAttr = vector<double>();

    bool allGained = true;
    for (int i = 0; i < vertexAllSum; i++) {
        if (execute.vSet[i].isActive) {
            // copy data
            cPlusReturnId.emplace_back(i);
            for (int j = 0; j < lenMarkID; j++) {
                cPlusReturnAttr.emplace_back(execute.vValues[i * lenMarkID + j]);
            }

            // detect if the vertex is filtered
            if(! execute.filteredV[i]){
                allGained = false;
            }
        }
    }

    string resultAttrIdentifier = to_string(pid) + "Double" + "Results";
    string resultIdIdentifier = to_string(pid) + "Long" + "Results";

    chk = writeShm(resultIdIdentifier, cPlusReturnId);
    if(chk == -1){
        throwIllegalArgument(env, "Cannot construct shared data memory");
    }

    chk = writeShm(resultAttrIdentifier, cPlusReturnAttr);
    if(chk == -1){
        throwIllegalArgument(env, "Cannot construct shared data memory");
    }

    execute.disconnect();

    bool ifReturnId = env->CallObjectMethod(shmReader, addReaderName, env->NewStringUTF(resultIdIdentifier.c_str()), cPlusReturnId.size());

    if(! ifReturnId){
        throwIllegalArgument(env, "Cannot write back identifier");
    }

    bool ifReturnAttr = env->CallObjectMethod(shmReader, addReaderName, env->NewStringUTF(resultAttrIdentifier.c_str()), cPlusReturnAttr.size());

    if(! ifReturnAttr){
        throwIllegalArgument(env, "Cannot write back identifier");
    }

    std::chrono::nanoseconds durationB = std::chrono::high_resolution_clock::now() - startTime;

    std::chrono::nanoseconds durationAll = std::chrono::high_resolution_clock::now() - startTimeAll;
    std::string output = std::string();
    output += "Time of partition " + to_string(pid) + " in c++: " + to_string(durationA.count()) + " "
              + to_string(duration.count()) + " " + to_string(durationB.count()) + " sum time: "
              + to_string(durationAll.count());

    cout<<output<<endl;

    if(allGained){
        return static_cast<int>(0-cPlusReturnId.size());
    }
    else{
        return static_cast<int>(cPlusReturnId.size());
    }

}

JNIEXPORT jint JNICALL Java_edu_ustc_nodb_PregelGPU_Algorithm_SSSPshm_GPUNativeShm_nativeSkipStep
        (JNIEnv * env, jobject superClass,
                jlong vertexSum, jint vertexLen, jint edgeLen, jint markIdLen, jint pid, jobject shmReader){

    auto startTimeB = std::chrono::high_resolution_clock::now();

    jclass readerClass = env->GetObjectClass(shmReader);
    jmethodID addReaderName = env->GetMethodID(readerClass, "addName", "(Ljava/lang/String;I)Z");

    int vertexAllSum = static_cast<int>(vertexSum);
    int partitionID = static_cast<int>(pid);
    int lenMarkID = static_cast<int>(markIdLen);
    int lenEdge = static_cast<int>(edgeLen);

    UtilClient<double, double> execute = UtilClient<double, double>(vertexAllSum, lenEdge, lenMarkID, partitionID);

    int chk = 0;

    chk = execute.connect();
    if (chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
    }

    execute.request();

    vector<long> cPlusReturnId = vector<long>();
    vector<double> cPlusReturnAttr = vector<double>();

    bool allGained = true;
    for (int i = 0; i < vertexAllSum; i++) {
        if (execute.vSet[i].isActive) {
            // copy data
            cPlusReturnId.emplace_back(i);
            for (int j = 0; j < lenMarkID; j++) {
                cPlusReturnAttr.emplace_back(execute.vValues[i * lenMarkID + j]);
            }

            // detect if the vertex is filtered
            if(! execute.filteredV[i]){
                allGained = false;
            }
        }
    }

    string resultAttrIdentifier = to_string(pid) + "Double" + "Results";
    string resultIdIdentifier = to_string(pid) + "Long" + "Results";

    chk = writeShm(resultIdIdentifier, cPlusReturnId);
    if(chk == -1){
        throwIllegalArgument(env, "Cannot construct shared data memory");
    }

    chk = writeShm(resultAttrIdentifier, cPlusReturnAttr);
    if(chk == -1){
        throwIllegalArgument(env, "Cannot construct shared data memory");
    }

    execute.disconnect();

    bool ifReturnId = env->CallObjectMethod(shmReader, addReaderName, env->NewStringUTF(resultIdIdentifier.c_str()), cPlusReturnId.size());

    if(! ifReturnId){
        throwIllegalArgument(env, "Cannot write back identifier");
    }

    bool ifReturnAttr = env->CallObjectMethod(shmReader, addReaderName, env->NewStringUTF(resultAttrIdentifier.c_str()), cPlusReturnAttr.size());

    if(! ifReturnAttr){
        throwIllegalArgument(env, "Cannot write back identifier");
    }

    std::chrono::nanoseconds durationB = std::chrono::high_resolution_clock::now() - startTimeB;

    std::string output = std::string();
    output += "Time of partition " + to_string(pid) + " in c++ for skipping: " + to_string(durationB.count());

    cout<<output<<endl;

    if(allGained){
        return static_cast<int>(0-cPlusReturnId.size());
    }
    else{
        return static_cast<int>(cPlusReturnId.size());
    }
}

JNIEXPORT jint JNICALL Java_edu_ustc_nodb_PregelGPU_Algorithm_SSSPshm_GPUNativeShm_nativeStepFinal
        (JNIEnv * env, jobject superClass,
                jlong vertexSum, jint vertexLen, jint edgeLen, jint markIdLen, jint pid, jobject shmReader) {

    auto startTimeB = std::chrono::high_resolution_clock::now();

    jclass readerClass = env->GetObjectClass(shmReader);
    jmethodID addReaderName = env->GetMethodID(readerClass, "addName", "(Ljava/lang/String;I)Z");

    int vertexAllSum = static_cast<int>(vertexSum);
    int partitionID = static_cast<int>(pid);
    int lenMarkID = static_cast<int>(markIdLen);
    int lenEdge = static_cast<int>(edgeLen);

    UtilClient<double, double> execute = UtilClient<double, double>(vertexAllSum, lenEdge, lenMarkID, partitionID);

    int chk = 0;

    chk = execute.connect();
    if (chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
    }

    vector<long> cPlusReturnId = vector<long>();
    vector<double> cPlusReturnAttr = vector<double>();

    for(int i = 0; i < vertexAllSum; i++){
        bool idFiltered = execute.filteredV[i];
        if(idFiltered){
            cPlusReturnId.emplace_back(i);
            for (int j = 0; j < lenMarkID; j++) {
                cPlusReturnAttr.emplace_back(execute.vValues[i * lenMarkID + j]);
            }
        }
    }

    string resultAttrIdentifier = to_string(pid) + "Double" + "Results";
    string resultIdIdentifier = to_string(pid) + "Long" + "Results";

    chk = writeShm(resultIdIdentifier, cPlusReturnId);
    if(chk == -1){
        throwIllegalArgument(env, "Cannot construct shared data memory");
    }

    chk = writeShm(resultAttrIdentifier, cPlusReturnAttr);
    if(chk == -1){
        throwIllegalArgument(env, "Cannot construct shared data memory");
    }

    execute.disconnect();

    bool ifReturnId = env->CallObjectMethod(shmReader, addReaderName, env->NewStringUTF(resultIdIdentifier.c_str()), cPlusReturnId.size());

    if(! ifReturnId){
        throwIllegalArgument(env, "Cannot write back identifier");
    }

    bool ifReturnAttr = env->CallObjectMethod(shmReader, addReaderName, env->NewStringUTF(resultAttrIdentifier.c_str()), cPlusReturnAttr.size());

    if(! ifReturnAttr){
        throwIllegalArgument(env, "Cannot write back identifier");
    }

    std::chrono::nanoseconds durationB = std::chrono::high_resolution_clock::now() - startTimeB;

    std::string output = std::string();
    output += "Time of partition " + to_string(pid) + " in c++ for all merging: " + to_string(durationB.count());

    cout<<output<<endl;

    return static_cast<int>(cPlusReturnId.size());

}


// server shutdown

JNIEXPORT jboolean JNICALL Java_edu_ustc_nodb_PregelGPU_Algorithm_SSSPshm_GPUNativeShm_nativeEnvClose
  (JNIEnv * env, jobject superClass, jint pid){

    UtilClient<double, double> control = UtilClient<double, double>(0, 0, 0, pid);

    int chk = control.connect();
    if (chk == -1){
        return false;
    }

    control.shutdown();
    return true;
}