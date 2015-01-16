//s-t shortest path on unweighted graph, by bi-directional BFS

#include "ol/pregel-ol-dev.h"
#include "utils/type.h"

string in_path = "/tmp/webuk";
string out_path = "/ol_out";
bool use_combiner = true;

//input line format: vertexID \t in_num in1 in2 ... out_num out1 out2 ...
//edge lengths are assumed to be 1

//output line format: met_vertex \t path_length

//--------------------------------------------------

//Step 1: define static field of vertex: adj-list
struct BiBFSValue {
    vector<int> in_nbs;
    vector<int> out_nbs;
};

ibinstream& operator<<(ibinstream& m, const BiBFSValue& v)
{
    m << v.in_nbs;
    m << v.out_nbs;
    return m;
}

obinstream& operator>>(obinstream& m, BiBFSValue& v)
{
    m >> v.in_nbs;
    m >> v.out_nbs;
    return m;
}

//--------------------------------------------------

//Step 2: define query type: here, it is intpair (src, dst)

//--------------------------------------------------
//Step 3: define query-specific vertex state:
//here, it is intpair (hop_to_src, hop_to_dst)

//--------------------------------------------------

//Step 4: define msg type: here, it is char
char fwd_msg = 1; //01
char back_msg = 2; //10
char met_msg = 3; //11

//--------------------------------------------------
//Step 5: define vertex class

int not_reached = -1;

class BiBFSVertex : public VertexOL<VertexID, intpair, BiBFSValue, char, intpair> {
public:
    //Step 5.1: define UDF1: query -> vertex's query-specific init state
    virtual intpair init_value(intpair& query)
    {
        intpair pair(not_reached, not_reached);
        if (id == query.v1)
            pair.v1 = 0;
        if (id == query.v2)
            pair.v2 = 0;
        return pair;
    }

    //Step 5.2: vertex.compute
    virtual void compute(MessageContainer& messages)
    {
        if (superstep() == 1) {
            intpair query = get_query();
            if (query.v1 == query.v2) {
                forceTerminate();
            } else {
                //forward broadcast
                if (id == query.v1) {
                    vector<int>& out_nbs = nqvalue().out_nbs;
                    for (int i = 0; i < out_nbs.size(); i++)
                        send_message(out_nbs[i], fwd_msg);
                }
                //backward broadcast
                if (id == query.v2) {
                    vector<int>& in_nbs = nqvalue().in_nbs;
                    for (int i = 0; i < in_nbs.size(); i++)
                        send_message(in_nbs[i], back_msg);
                }
            }
        } else {
            char bor = 0;
            for (int i = 0; i < messages.size(); i++) {
                bor |= messages[i];
                if (bor == met_msg)
                    break;
            }
            int hop_src = qvalue().v1;
            if ((bor & fwd_msg) != 0) //recv msgs from forward propagation
            {
                if (hop_src == not_reached) {
                    qvalue().v1 = superstep() - 1;
                    //forward broadcast
                    vector<int>& out_nbs = nqvalue().out_nbs;
                    for (int i = 0; i < out_nbs.size(); i++)
                        send_message(out_nbs[i], fwd_msg);
                }
            }
            int hop_dst = qvalue().v2;
            if ((bor & back_msg) != 0) //recv msgs from backward propagation
            {
                if (hop_dst == not_reached) {
                    qvalue().v2 = superstep() - 1;
                    //backward broadcast
                    vector<int>& in_nbs = nqvalue().in_nbs;
                    for (int i = 0; i < in_nbs.size(); i++)
                        send_message(in_nbs[i], back_msg);
                }
            }
            //check met?
            if ((hop_src != not_reached) && (hop_dst != not_reached)) {
                forceTerminate();
            }
        }
        vote_to_halt();
    }
};

//--------------------------------------------------
//Step 6: define worker class
class BiBFSWorkerOL : public WorkerOL<BiBFSVertex> {
public:
    char buf[50];

    //Step 6.1: UDF: line -> vertex
    virtual BiBFSVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        BiBFSVertex* v = new BiBFSVertex;
        int id = atoi(pch);
        v->id = id;
        pch = strtok(NULL, " ");
        int in_num = atoi(pch);
        for (int i = 0; i < in_num; i++) {
            pch = strtok(NULL, " ");
            int nb = atoi(pch);
            v->nqvalue().in_nbs.push_back(nb);
        }
        pch = strtok(NULL, " ");
        int out_num = atoi(pch);
        for (int i = 0; i < out_num; i++) {
            pch = strtok(NULL, " ");
            int nb = atoi(pch);
            v->nqvalue().out_nbs.push_back(nb);
        }
        return v;
    }

    //Step 6.2: UDF: query string -> query (src_id)
    virtual intpair toQuery(char* line)
    {
        char* pch;
        pch = strtok(line, " ");
        int src = atoi(pch);
        pch = strtok(NULL, " ");
        int dst = atoi(pch);
        return intpair(src, dst);
    }

    //Step 6.3: UDF: vertex init
    virtual void init(VertexContainer& vertex_vec)
    {
        int src = get_query().v1;
        int pos = get_vpos(src);
        if (pos != -1)
            activate(pos);
        //------
        int dst = get_query().v2;
        pos = get_vpos(dst);
        if (pos != -1)
            activate(pos);
    }

    //Step 6.4: UDF: task_dump
    virtual void dump(BiBFSVertex* vertex, BufferedWriter& writer)
    {
        intpair pair = vertex->qvalue();
        if ((pair.v1 != not_reached) && (pair.v2 != not_reached)) {
            sprintf(buf, "%d\t%d\n", vertex->id, vertex->qvalue().v1 + vertex->qvalue().v2);
            writer.write(buf);
        }
    }
};

class BiBFSCombiner : public Combiner<char> {
public:
    virtual void combine(char& old, const char& new_msg)
    {
        old |= new_msg;
    }
};

int main(int argc, char* argv[])
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    BiBFSWorkerOL worker;
    BiBFSCombiner combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    worker.run(param);
    return 0;
}
