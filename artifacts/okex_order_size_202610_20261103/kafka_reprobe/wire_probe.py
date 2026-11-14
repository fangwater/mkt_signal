"""Bounded read-only Kafka Metadata v0 probe; does not create topics or join groups."""
import argparse, socket, struct, json, time, datetime
from pathlib import Path


def string(s):
    b=s.encode()
    return struct.pack('>h',len(b))+b

class Reader:
    def __init__(self,b): self.b,self.i=b,0
    def num(self,fmt):
        n=struct.calcsize('>'+fmt)
        v=struct.unpack_from('>'+fmt,self.b,self.i)[0]
        self.i+=n
        return v
    def text(self):
        n=self.num('h')
        if n<0: return None
        v=self.b[self.i:self.i+n].decode()
        self.i+=n
        return v
    def array(self,fn): return [fn() for _ in range(self.num('i'))]

def recv_exact(s,n):
    b=bytearray()
    while len(b)<n:
        chunk=s.recv(n-len(b))
        if not chunk: raise EOFError('Kafka socket closed')
        b.extend(chunk)
    return bytes(b)

def metadata(host,port):
    cid=43117
    req=struct.pack('>hhi',3,0,cid)+string('okex-readonly-metadata')+struct.pack('>i',0)
    t=time.monotonic()
    with socket.create_connection((host,port),timeout=6) as s:
        s.settimeout(8)
        s.sendall(struct.pack('>i',len(req))+req)
        size=struct.unpack('>i',recv_exact(s,4))[0]
        if size<4 or size>10_000_000: raise ValueError(f'Invalid metadata size {size}')
        payload=recv_exact(s,size)
    r=Reader(payload)
    assert r.num('i')==cid
    brokers=r.array(lambda: {'id':r.num('i'),'host':r.text(),'port':r.num('i')})
    def partition():
        return {'error':r.num('h'),'id':r.num('i'),'leader':r.num('i'),'replicas':r.array(lambda:r.num('i')),'isr':r.array(lambda:r.num('i'))}
    topics=r.array(lambda:{'error':r.num('h'),'name':r.text(),'partitions':r.array(partition)})
    assert r.i==len(payload)
    return {'observed_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'bootstrap':f'{host}:{port}','elapsed_seconds':time.monotonic()-t,'brokers':brokers,'topics':topics}

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--host',default='156.231.137.189')
    parser.add_argument('--port',type=int,default=1501)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    try: result=metadata(args.host,args.port)
    except Exception as exc: result={'error':type(exc).__name__,'message':str(exc)}
    raw=json.dumps(result,indent=2)+'\n'
    if args.output: args.output.write_text(raw)
    print(raw)
