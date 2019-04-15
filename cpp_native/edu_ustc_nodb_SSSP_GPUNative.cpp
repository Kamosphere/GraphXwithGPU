#include "edu_ustc_nodb_SSSP_GPUNative.h"
#include <vector>
#include <map>
#include <iostream>
#include <string>
#include "Graph_Algo/core/Graph.h"
#include "Graph_Algo/algo/BellmanFord/BellmanFord.h"
#include "Graph_Algo/srv/UtilServer.h"
#include "Graph_Algo/srv/UtilClient.h"
#include <cstdlib>
#include <chrono>

using namespace std;

// throw error to JVM

jint throwNoClassDefError( JNIEnv *env, const char *message )
{
    jclass exClass;
    const char *className = "java/lang/NoClassDefFoundError";

    exClass = env->FindClass(className);
    if (exClass == nullptr) {
        return throwNoClassDefError( env, className );
    }

    return env->ThrowNew( exClass, message );
}

// throw error to JVM

jint throwIllegalArgument( JNIEnv *env, const char *message )
{
    jclass exClass;
    const char *className = "java/lang/IllegalArgumentException" ;

    exClass = env->FindClass( className );
    if ( exClass == nullptr ) {
        return throwNoClassDefError( env, className );
    }

    return env->ThrowNew( exClass, message );
}

// Init the edge and markID

JNIEXPORT jboolean JNICALL Java_edu_ustc_nodb_SSSP_GPUNative_GPUServerInit
(JNIEnv * env, jobject superClass,
        jlong vertexNum, jlongArray jEdgeSrc, jlongArray jEdgeDst, jdoubleArray jEdgeAttr,
        jobject markId, jint pid){

    jclass c_Long = env->FindClass("java/lang/Long");
    jmethodID longValue = env->GetMethodID(c_Long, "longValue", "()J");

    jclass c_ArrayList = env->FindClass("java/util/ArrayList");
    jmethodID id_ArrayList_size = env->GetMethodID(c_ArrayList, "size", "()I");
    jmethodID id_ArrayList_get = env->GetMethodID(c_ArrayList, "get", "(I)Ljava/lang/Object;");

    //---------Entity---------

    int lenMarkID = env->CallIntMethod(markId, id_ArrayList_size);
    int lenEdge = env->GetArrayLength(jEdgeSrc);

    int vertexNumbers = static_cast<int>(vertexNum);
    int partitionID = static_cast<int>(pid);

    //Init the Graph with blank vertices

    vector<Vertex> vertices = vector<Vertex>();
    double *vValues = new double [vertexNumbers * lenMarkID];

    vector<Edge> edges = vector<Edge>();

    for(int i = 0; i < vertexNumbers; i++){
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

    long* EdgeSrcTemp = env->GetLongArrayElements(jEdgeSrc, nullptr);
    long* EdgeDstTemp = env->GetLongArrayElements(jEdgeDst, nullptr);
    double * EdgeAttrTemp = env->GetDoubleArrayElements(jEdgeAttr, nullptr);

    for (int i = 0; i < lenEdge; i++) {

        int jSrcId_get = EdgeSrcTemp[i];
        //jlong SrcId_get = env->CallLongMethod(jSrcId, longValue);
        int jDstId_get = EdgeDstTemp[i];
        //jlong DstId_get = env->CallLongMethod(jDstId, longValue);
        double jAttr_get = EdgeAttrTemp[i];
        //jdouble Attr_get = env->CallDoubleMethod(jAttr, doubleValue);

        edges.emplace_back(Edge(jSrcId_get, jDstId_get, jAttr_get));

    }

    UtilClient execute = UtilClient(vertexNumbers, lenEdge, lenMarkID, partitionID);

    int chk = 0;

    chk = execute.connect();
    if (chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
        return false;
    }

    chk = execute.transfer(vValues, &vertices[0], &edges[0], initVSet);

    if(chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
        return false;
    }

    execute.disconnect();

    return true;
}


JNIEXPORT jobject JNICALL Java_edu_ustc_nodb_SSSP_GPUNative_GPUClientSSSP
(JNIEnv * env, jobject superClass,
        jlong vertexNum, jlongArray jVertexId, jbooleanArray jVertexActive, jdoubleArray jVertexAttr,
        jint edgeLen, jint markIdLen, jint pid){

    jclass c_ArrayList = env->FindClass("java/util/ArrayList");
    jmethodID id_ArrayList_size = env->GetMethodID(c_ArrayList, "size", "()I");
    jmethodID id_ArrayList_get = env->GetMethodID(c_ArrayList, "get", "(I)Ljava/lang/Object;");

    jclass n_VertexSet = env->FindClass("edu/ustc/nodb/SSSP/VertexSet");
    jmethodID id_VertexSet_VertexId = env->GetMethodID(n_VertexSet, "VertexId", "()J");
    jmethodID id_VertexSet_ifActive = env->GetMethodID(n_VertexSet, "ifActive", "()Z");
    jmethodID id_VertexSet_Attr = env->GetMethodID(n_VertexSet, "Attr", "()Ljava/util/HashMap;");
    jmethodID id_VertexSet_addAttr = env->GetMethodID(n_VertexSet, "addAttr", "(JD)V");
    jmethodID id_VertexSet_TupleReturn = env->GetMethodID(n_VertexSet, "TupleReturn", "()Lscala/Tuple2;");
    jmethodID VertexSetConstructor = env->GetMethodID(n_VertexSet, "<init>", "(JZ)V");

    jclass c_ArrayBuffer = env->FindClass("scala/collection/mutable/ArrayBuffer");
    jmethodID ArrayBufferConstructor = env->GetMethodID(c_ArrayBuffer, "<init>", "()V");
    jmethodID id_ArrayBuffer_pluseq = env->GetMethodID(c_ArrayBuffer, "$plus$eq",
                                                       "(Ljava/lang/Object;)Lscala/collection/mutable/ArrayBuffer;");

    jclass c_EntrySet = env->FindClass("java/util/Set");
    jmethodID id_iterator = env->GetMethodID(c_EntrySet, "iterator", "()Ljava/util/Iterator;");

    jclass c_Iterator = env->FindClass("java/util/Iterator");
    jmethodID id_hasNext = env->GetMethodID(c_Iterator, "hasNext", "()Z");
    jmethodID id_next = env->GetMethodID(c_Iterator, "next", "()Ljava/lang/Object;");

    jclass c_map = env->FindClass("java/util/Map");
    jmethodID id_attrArr_EntrySet = env->GetMethodID(c_map, "entrySet", "()Ljava/util/Set;");

    jclass c_Entry = env->FindClass("java/util/Map$Entry");
    jmethodID id_getKey = env->GetMethodID(c_Entry, "getKey", "()Ljava/lang/Object;");
    jmethodID id_getValue = env->GetMethodID(c_Entry, "getValue", "()Ljava/lang/Object;");

    jclass c_Long = env->FindClass("java/lang/Long");
    jmethodID longValue = env->GetMethodID(c_Long, "longValue", "()J");

    jclass c_Double = env->FindClass("java/lang/Double");
    jmethodID doubleValue = env->GetMethodID(c_Double, "doubleValue", "()D");

    // jclass n_sztool = env->FindClass("edu/ustc/nodb/matrix/SizesTool");
    // jmethodID id_getSize = env->GetStaticMethodID(n_sztool, "getObjectSize", "(Ljava/lang/Object;)J");

    //---------Entity---------

    int vertexNumbers = static_cast<int>(vertexNum);
    int partitionID = static_cast<int>(pid);
    int lenMarkID = static_cast<int>(markIdLen);
    int lenEdge = static_cast<int>(edgeLen);

    UtilClient execute = UtilClient(vertexNumbers, lenEdge, lenMarkID, partitionID);

    int chk = 0;

    chk = execute.connect();
    if (chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
    }

    // the quantity of vertices

    int lenVertex = env->GetArrayLength(jVertexId);

    //Init the vertices

    vector<Vertex> vertices = vector<Vertex>();
    double *vValues = new double [vertexNumbers * lenMarkID];

    for(int i = 0; i < vertexNumbers; i++){
        vertices.emplace_back(Vertex(i, false, INVALID_INITV_INDEX));
        for(int j = 0; j < lenMarkID; j++){
            vValues[i * lenMarkID + j] = (INT32_MAX >> 1);
        }
    }

    for(jint i = 0; i < lenMarkID; i++){
        vertices.at(execute.initVSet[i]).initVIndex = i;
    }

    // fill vertices attributes
    long* VertexIDTemp = env->GetLongArrayElements(jVertexId, nullptr);
    jboolean * VertexActiveTemp = env->GetBooleanArrayElements(jVertexActive, nullptr);
    double* VertexAttrTemp = env->GetDoubleArrayElements(jVertexAttr, nullptr);

    for (int i = 0; i < lenVertex; i++) {

        // jlong sig = env->CallStaticLongMethod(n_sztool, id_getSize, vertexLL);

        long jVertexId_get = VertexIDTemp[i];

        for(int j = 0; j < lenMarkID; j++){
            double jDis = VertexAttrTemp[i * lenMarkID + j] ;
            int index = vertices.at(execute.initVSet[j]).initVIndex;
            if(index != INVALID_INITV_INDEX)vValues[jVertexId_get * lenMarkID + index] = jDis;
        }

        bool jVertexActive_get = VertexActiveTemp[i];

        vertices.at(jVertexId_get).isActive = jVertexActive_get;

    }

/*
    // test for multithreading environment
    std::string output = std::string();

    for(int i = 0;i< vertexNumbers;i++){
        output += to_string(i) + ": {";
        for(int j = 0;j<MarkIDCount;j++){
            output += " [" + to_string(initVSet[j])+": "+to_string(vValues[i * MarkIDCount + j]) + "] ";
        }
        output += "}";
    }

    std::cout<<output<<std::endl;

    output.clear();
    // test end
*/

    // auto startTime = std::chrono::high_resolution_clock::now();

    // execute sssp using GPU in server-client mode

    chk = execute.update(vValues, &vertices[0]);

    if(chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
    }

    execute.request();

    //std::chrono::nanoseconds duration = std::chrono::high_resolution_clock::now() - startTime;

    jobject vertexSubModified = env->NewObject(c_ArrayBuffer, ArrayBufferConstructor);

    for(int i = 0; i < vertexNumbers; i++){

        if(false | execute.vSet[i].isActive){

            jlong messageVid = i;
            jboolean messageActive = true;
            jobject messageUnit = env->NewObject(n_VertexSet, VertexSetConstructor, messageVid, messageActive);

            for (int j = 0; j < lenMarkID; j++) {

                jlong messageDstId = execute.initVSet[j];
                jdouble messageDist = execute.vValues[i * lenMarkID + j];
                env->CallObjectMethod(messageUnit, id_VertexSet_addAttr, messageDstId, messageDist);

            }

            // run the method to get the returned pair, no need to arrange data in another loop
            jobject TupleUnit = env->CallObjectMethod(messageUnit, id_VertexSet_TupleReturn);

            env->CallObjectMethod(vertexSubModified, id_ArrayBuffer_pluseq, TupleUnit);

            env->DeleteLocalRef(messageUnit);
            env->DeleteLocalRef(TupleUnit);
        }
    }

    execute.disconnect();
    return vertexSubModified;
}

// server shutdown

JNIEXPORT jboolean JNICALL Java_edu_ustc_nodb_SSSP_GPUNative_GPUServerShutdown
  (JNIEnv * env, jobject superClass, jint pid){
    UtilClient control = UtilClient(0, 0, 0, pid);

    int chk = control.connect();
    if (chk == -1)
    {
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
        return false;
    }

    control.shutdown();
    return true;
}