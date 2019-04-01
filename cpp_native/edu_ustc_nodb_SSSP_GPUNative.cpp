#include "edu_ustc_nodb_SSSP_GPUNative.h"
#include <vector>
#include <map>
#include <iostream>
#include <string>
#include "Graph_Algo/core/Graph.h"
#include "Graph_Algo/algo/BellmanFord/BellmanFord.h"
#include <cstdlib>
#include <chrono>

using namespace std;

// get c++ map through JNI
// even though the elements in HashMap is defined as basic type, data still need to unBox.

map<int, double> generate_vertexAttr_map(JNIEnv * env, jobject& attrArr){

    jclass c_EntrySet = env->FindClass("java/util/Set");
    jmethodID id_iterator = env->GetMethodID(c_EntrySet, "iterator", "()Ljava/util/Iterator;");

    jclass c_Iterator = env->FindClass("java/util/Iterator");
    jmethodID id_hasNext = env->GetMethodID(c_Iterator, "hasNext", "()Z");
    jmethodID id_next = env->GetMethodID(c_Iterator, "next", "()Ljava/lang/Object;");

    jclass c_Entry = env->FindClass("java/util/Map$Entry");
    jmethodID id_getKey = env->GetMethodID(c_Entry, "getKey", "()Ljava/lang/Object;");
    jmethodID id_getValue = env->GetMethodID(c_Entry, "getValue", "()Ljava/lang/Object;");

    jclass c_Long = env->FindClass("java/lang/Long");
    jmethodID longValue = env->GetMethodID(c_Long, "longValue", "()J");

    jclass c_Double = env->FindClass("java/lang/Double");
    jmethodID doubleValue = env->GetMethodID(c_Double, "doubleValue", "()D");

    jclass c_attrArr_map = env->GetObjectClass(attrArr);
    jmethodID id_attrArr_EntrySet = env->GetMethodID(c_attrArr_map, "entrySet", "()Ljava/util/Set;");

    //---------Entity---------

    jobject obj_EntrySet = env->CallObjectMethod(attrArr, id_attrArr_EntrySet);
    jobject obj_Iterator = env->CallObjectMethod(obj_EntrySet, id_iterator);

    bool HasNext = (bool) env->CallBooleanMethod(obj_Iterator, id_hasNext);

    map<int, double> attrMap;

    while (HasNext) {

        jobject entry = env->CallObjectMethod(obj_Iterator, id_next);

        jobject key = env->CallObjectMethod(entry, id_getKey);
        jobject value = env->CallObjectMethod(entry, id_getValue);

        int jVid = static_cast<int>(env->CallLongMethod(key, longValue)) ;
        double jDis = env->CallDoubleMethod(value, doubleValue);

        attrMap.insert(std::pair<int, double>(jVid, jDis));

        HasNext = (bool) env->CallBooleanMethod(obj_Iterator, id_hasNext);

        env->DeleteLocalRef(entry);
        env->DeleteLocalRef(key);
        env->DeleteLocalRef(value);
    }
    env->DeleteLocalRef(obj_Iterator);
    env->DeleteLocalRef(obj_EntrySet);

    return attrMap;

}

JNIEXPORT jobject JNICALL Java_edu_ustc_nodb_SSSP_GPUNative_GPUSSSP
  (JNIEnv * env, jobject superClass,
          jlong vertexNum, jobject vertexLL, jobject edgeLL, jobject markId) {

    jclass c_ArrayList = env->FindClass("java/util/ArrayList");
    jmethodID id_ArrayList_size = env->GetMethodID(c_ArrayList, "size", "()I");
    jmethodID id_ArrayList_get = env->GetMethodID(c_ArrayList, "get", "(I)Ljava/lang/Object;");
    jmethodID id_ArrayList_add = env->GetMethodID(c_ArrayList, "add", "(Ljava/lang/Object;)Z");
    jmethodID ArrayListConstructor = env->GetMethodID(c_ArrayList, "<init>", "()V");

    jclass n_VertexSet = env->FindClass("edu/ustc/nodb/SSSP/VertexSet");
    jmethodID id_VertexSet_VertexId = env->GetMethodID(n_VertexSet, "VertexId", "()J");
    jmethodID id_VertexSet_ifActive = env->GetMethodID(n_VertexSet, "ifActive", "()Z");
    jmethodID id_VertexSet_Attr = env->GetMethodID(n_VertexSet, "Attr", "()Ljava/util/HashMap;");
    jmethodID id_VertexSet_addAttr = env->GetMethodID(n_VertexSet, "addAttr", "(JD)V");
    jmethodID VertexSetConstructor = env->GetMethodID(n_VertexSet, "<init>", "(JZ)V");

    jclass n_EdgeSet = env->FindClass("edu/ustc/nodb/SSSP/EdgeSet");
    jmethodID id_EdgeSet_SrcId = env->GetMethodID(n_EdgeSet, "SrcId", "()J");
    jmethodID id_EdgeSet_DstId = env->GetMethodID(n_EdgeSet, "DstId", "()J");
    jmethodID id_EdgeSet_Attr = env->GetMethodID(n_EdgeSet, "Attr", "()D");

    /* unused signature

    jclass c_Long = env->FindClass("java/lang/Long");
    jmethodID longValue = env->GetMethodID(c_Long, "longValue", "()J");
    jmethodID LongConstructor = env->GetMethodID(c_Long, "<init>", "(J)V");

    jclass c_Double = env->FindClass("java/lang/Double");
    jmethodID doubleValue = env->GetMethodID(c_Double, "doubleValue", "()D");
    jmethodID DoubleConstructor = env->GetMethodID(c_Double, "<init>", "(D)V");

    jclass c_Boolean = env->FindClass("java/lang/Boolean");
    jmethodID boolValue = env->GetMethodID(c_Boolean, "booleanValue", "()Z");
    jmethodID BooleanConstructor = env->GetMethodID(c_Boolean, "<init>", "(Z)V");

    jclass c_HashMap = env->FindClass("java/util/HashMap");
    jmethodID MapConstructor = env->GetMethodID(c_HashMap, "<init>", "()V");
    jmethodID id_map_put = env->GetMethodID(c_HashMap, "put",
                                            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    */

    //---------Entity---------

    vector<Edge> edges;
    map<int, map<int, double>> vertices;

    set<int> markID;
    set<int> actMessageID;

    // the quantity of vertices in the whole graph
    int vertexNumbers = static_cast<int>(vertexNum);

    int lenMarkID = env->CallIntMethod(markId, id_ArrayList_size);

    // fill markID, which stored the landmark

    for(jint i = 0; i < lenMarkID; i++){

        jlong start = env->CallLongMethod(markId, id_ArrayList_get, i);
        //jlong jMarkIDUnit = env->CallLongMethod(start, longValue);
        markID.insert(static_cast<int>(start));

    }

    int lenVertex = env->CallIntMethod(vertexLL, id_ArrayList_size);

    // fill vertices attributes

    for (jint i = 0; i < lenVertex; i++) {

        jobject start = env->CallObjectMethod(vertexLL, id_ArrayList_get, i);

        jlong jVertexId_get = env->CallLongMethod(start, id_VertexSet_VertexId);
        //jlong jVertexId_get = env->CallLongMethod(jVertexId, longValue);

        jobject jVertexAttr = env->CallObjectMethod(start, id_VertexSet_Attr);
        map<int, double> jVertexAttr_get = generate_vertexAttr_map(env, jVertexAttr);
        vertices.insert(pair<int, map<int,double>>(static_cast<int>(jVertexId_get), jVertexAttr_get));

        bool jVertexActive_get = env->CallBooleanMethod(start, id_VertexSet_ifActive);
        //bool jVertexActive_get = env->CallBooleanMethod(jVertexActive, boolValue);

        //build active messages, which is used in graph constructor
        if(jVertexActive_get) actMessageID.insert(static_cast<int>(jVertexId_get));

        env->DeleteLocalRef(start);
        env->DeleteLocalRef(jVertexAttr);
    }

    int lenEdge = env->CallIntMethod(edgeLL, id_ArrayList_size);

    // fill edges

    for (jint i = 0; i < lenEdge; i++) {

        jobject start = env->CallObjectMethod(edgeLL, id_ArrayList_get, i);

        jlong jSrcId_get = env->CallLongMethod(start, id_EdgeSet_SrcId);
        //jlong SrcId_get = env->CallLongMethod(jSrcId, longValue);
        jlong jDstId_get = env->CallLongMethod(start, id_EdgeSet_DstId);
        //jlong DstId_get = env->CallLongMethod(jDstId, longValue);
        jdouble jAttr_get = env->CallDoubleMethod(start, id_EdgeSet_Attr);
        //jdouble Attr_get = env->CallDoubleMethod(jAttr, doubleValue);

        edges.emplace_back(Edge(static_cast<int>(jSrcId_get), static_cast<int>(jDstId_get),jAttr_get));

        env->DeleteLocalRef(start);
    }

    /*
    // test for multithreading environment
    std::string output = std::string();

    for(auto ou : vertices){
        output += std::to_string(ou.first) + ": ";
        for(auto ov : ou.second){
            if(ov.second>114514) ov.second = 114514;
            output += "["+std::to_string(ov.first)+"->"+std::to_string(ov.second)+"] ";
        }
    }

    std::cout<<output<<std::endl;

    output.clear();
    // test end
    */

    // execute the sssp

    Graph g = Graph(vertexNumbers, vertices, edges, actMessageID, markID);

    BellmanFord executor = BellmanFord();

    // deployment in CPU version is unnecessary
    executor.Deploy(g, static_cast<int>(markID.size()));

    // main SSSP step (CPU version) (time consume counting)
    //auto startTime = std::chrono::high_resolution_clock::now();
    executor.ApplyStep(g, actMessageID);
    //std::chrono::nanoseconds duration = std::chrono::high_resolution_clock::now() - startTime;
    //std::cout<<"time: "<<duration.count()<<std::endl;

    //after executing, package the vertex information

    //int sizeReturned = static_cast<int>(actMessageID.size());

    jobject vertexSubModified = env->NewObject(c_ArrayList, ArrayListConstructor);

    for (const auto &it : g.vList) {

        //only pass the modified attr
        if(it.isActive){

            jlong messageVid = it.vertexID;
            jboolean messageActive = it.isActive;
            jobject messageUnit = env->NewObject(n_VertexSet, VertexSetConstructor, messageVid, messageActive);

            //using the inner method of VertexSet to add vertices active messages
            for (auto inner : it.value) {
                jlong messageDstId = inner.first;
                jdouble messageDist = inner.second;
                env->CallObjectMethod(messageUnit, id_VertexSet_addAttr, messageDstId, messageDist);
            }

            env->CallObjectMethod(vertexSubModified, id_ArrayList_add, messageUnit);
        }

    }

    // free in CPU version is unnecessary
    executor.Free(g);
    return vertexSubModified;
}
