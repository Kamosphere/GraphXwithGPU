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
        (JNIEnv * env, jobject superClass, jlong vertexNum, jobject edgeLL, jobject markId, jint pid){

    jclass c_Long = env->FindClass("java/lang/Long");
    jmethodID longValue = env->GetMethodID(c_Long, "longValue", "()J");

    jclass c_ArrayList = env->FindClass("java/util/ArrayList");
    jmethodID id_ArrayList_size = env->GetMethodID(c_ArrayList, "size", "()I");
    jmethodID id_ArrayList_get = env->GetMethodID(c_ArrayList, "get", "(I)Ljava/lang/Object;");

    jclass n_EdgeSet = env->FindClass("edu/ustc/nodb/SSSP/EdgeSet");
    jmethodID id_EdgeSet_SrcId = env->GetMethodID(n_EdgeSet, "SrcId", "()J");
    jmethodID id_EdgeSet_DstId = env->GetMethodID(n_EdgeSet, "DstId", "()J");
    jmethodID id_EdgeSet_Attr = env->GetMethodID(n_EdgeSet, "Attr", "()D");

    //---------Entity---------

    int lenMarkID = env->CallIntMethod(markId, id_ArrayList_size);
    int lenEdge = env->CallIntMethod(edgeLL, id_ArrayList_size);

    int vertexNumbers = static_cast<int>(vertexNum);
    int partitionID = static_cast<int>(pid);

    // fill markID, which stored the landmark

    int *initVSet = new int [lenMarkID];

    for(jint i = 0; i < lenMarkID; i++){

        jobject start = env->CallObjectMethod(markId, id_ArrayList_get, i);
        jlong jMarkIDUnit = env->CallLongMethod(start, longValue);
        initVSet[i] = jMarkIDUnit;

    }

    //Init the Graph with blank vertices

    bool *AVCheckSet = new bool [vertexNumbers];
    double *vValues = new double [vertexNumbers * lenMarkID];

    int *eSrcSet = new int [lenEdge];
    int *eDstSet = new int [lenEdge];
    double *eWeightSet = new double [lenEdge];

    for(int i = 0; i < vertexNumbers; i++){
        AVCheckSet[i]=false;
        for(int j = 0; j < lenMarkID; j++){
            vValues[i * lenMarkID + j] = (INT32_MAX >> 1);
        }
    }

    // Init the Graph with existed edges

    for (jint i = 0; i < lenEdge; i++) {

        jobject start = env->CallObjectMethod(edgeLL, id_ArrayList_get, i);

        int jSrcId_get = env->CallLongMethod(start, id_EdgeSet_SrcId);
        //jlong SrcId_get = env->CallLongMethod(jSrcId, longValue);
        int jDstId_get = env->CallLongMethod(start, id_EdgeSet_DstId);
        //jlong DstId_get = env->CallLongMethod(jDstId, longValue);
        double jAttr_get = env->CallDoubleMethod(start, id_EdgeSet_Attr);
        //jdouble Attr_get = env->CallDoubleMethod(jAttr, doubleValue);

        eSrcSet[i]=jSrcId_get;
        eDstSet[i]=jDstId_get;
        eWeightSet[i]=jAttr_get;

        env->DeleteLocalRef(start);
    }

    UtilClient execute = UtilClient(vertexNumbers, lenEdge, lenMarkID, partitionID);

    int chk = 0;

    chk = execute.connect();
    if (chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
        return false;
    }

    chk = execute.transfer(vValues, eSrcSet, eDstSet, eWeightSet, AVCheckSet, initVSet);

    if(chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
        return false;
    }

    execute.disconnect();

    return true;
}


JNIEXPORT jobject JNICALL Java_edu_ustc_nodb_SSSP_GPUNative_GPUClientSSSP
  (JNIEnv * env, jobject superClass,
          jlong vertexNum, jobject vertexLL, jint edgeLen, jint markIdLen, jint pid) {

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

    int lenVertex = env->CallIntMethod(vertexLL, id_ArrayList_size);

    //Init the vertices

    bool *AVCheckSet = new bool [vertexNumbers];
    double *vValues = new double [vertexNumbers * lenMarkID];

    for(int i = 0; i < vertexNumbers; i++){
        AVCheckSet[i]=false;
        for(int j = 0; j < lenMarkID; j++){
            vValues[i * lenMarkID + j] = (INT32_MAX >> 1);
        }
    }

    // fill vertices attributes

    for (jint i = 0; i < lenVertex; i++) {

        jobject start = env->CallObjectMethod(vertexLL, id_ArrayList_get, i);
        // jlong sig = env->CallStaticLongMethod(n_sztool, id_getSize, vertexLL);

        jlong jVertexId_get = env->CallLongMethod(start, id_VertexSet_VertexId);

        jobject jVertexAttr = env->CallObjectMethod(start, id_VertexSet_Attr);

        jobject obj_EntrySet = env->CallObjectMethod(jVertexAttr, id_attrArr_EntrySet);
        jobject obj_Iterator = env->CallObjectMethod(obj_EntrySet, id_iterator);
        bool HasNext = (bool) env->CallBooleanMethod(obj_Iterator, id_hasNext);

        while (HasNext) {

            jobject entry = env->CallObjectMethod(obj_Iterator, id_next);

            jobject key = env->CallObjectMethod(entry, id_getKey);
            jobject value = env->CallObjectMethod(entry, id_getValue);

            int jVid = static_cast<int>(env->CallLongMethod(key, longValue)) ;
            double jDis = env->CallDoubleMethod(value, doubleValue);

            int index = -1;
            int j = 0;
            while(j < lenMarkID){
                if(execute.initVSet[j] == jVid) {
                    index = j;
                    break;
                }
                j++;
            }
            if(index != -1)vValues[jVertexId_get * lenMarkID + index] = jDis;

            HasNext = (bool) env->CallBooleanMethod(obj_Iterator, id_hasNext);

            env->DeleteLocalRef(entry);
            env->DeleteLocalRef(key);
            env->DeleteLocalRef(value);
        }

        env->DeleteLocalRef(obj_Iterator);
        env->DeleteLocalRef(obj_EntrySet);

        bool jVertexActive_get = env->CallBooleanMethod(start, id_VertexSet_ifActive);
        //bool jVertexActive_get = env->CallBooleanMethod(jVertexActive, boolValue);

        AVCheckSet[jVertexId_get]=jVertexActive_get;

        env->DeleteLocalRef(start);
        env->DeleteLocalRef(jVertexAttr);
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

    chk = execute.update(vValues, AVCheckSet);

    if(chk == -1){
        throwIllegalArgument(env, "Cannot establish the connection with server correctly");
    }

    execute.request();

    //Collect data
    for(int j = 0; j < vertexNumbers * lenMarkID; j++){
        if (execute.vValues[j] < vValues[j])
            vValues[j] = execute.vValues[j];
    }

    for(int j = 0; j < vertexNumbers; j++){
        AVCheckSet[j] = false | execute.AVCheckSet[j];
    }

    //std::chrono::nanoseconds duration = std::chrono::high_resolution_clock::now() - startTime;

    jobject vertexSubModified = env->NewObject(c_ArrayBuffer, ArrayBufferConstructor);

    for(int i = 0; i < vertexNumbers; i++){

        if(AVCheckSet[i]){

            jlong messageVid = i;
            jboolean messageActive = true;
            jobject messageUnit = env->NewObject(n_VertexSet, VertexSetConstructor, messageVid, messageActive);

            for (int j = 0; j < lenMarkID; j++) {

                jlong messageDstId = execute.initVSet[j];
                jdouble messageDist = vValues[i * lenMarkID + j];
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