import mmap, time, multiprocessing, datetime, platform, os, collections
import cPickle as pickle

_debug = 1
_sharedcachegap = 128
_sharedcachemaxsize = 256 + _sharedcachegap
_sharedcachereserve = 128 + _sharedcachegap
_sharedcacheheadsize = 128 + _sharedcachegap
_sharedcachewatermark = 5
if "Linux" == platform.system():
    _sharedcachepath = "/dev/shm/shc/"
if "Windows" == platform.system():
    _sharedcachepath = "c:\\tmp\\shc\\"

class SharedObject(object):
    """currently the read and write lock is used per object(prot_write | prot_read)
       the object could be dict list and any type
       if the performance is not enough to feed the requirement,
       please use the cpickle or reimplemnt the lower stack with ctype buffer.
       NOTE: the shared object shoule not be more than 200000 on each system(tested on 32-bit system),
             each shared object should not have more than 6000 entry(performance is low)
       TEST RESULT: read and write 100000 object consuming 0:00:06.325695"""
    def __init__(self, objectName, objectSize):
        self._name = _sharedcachepath + objectName
        self._size = objectSize
        self._live = 0
        self._fp = 0
        self._sysstr = platform.system()

    def createObject(self):
        fpath, fname = os.path.split(self._name)
        if not (os.path.exists(fpath) or '' == fpath):
            try:
                os.mkdir(fpath)
                self._fp = open(self._name, "w+")
            except:
                try:
                    self._fp = open(fname, "w+")
                    if _debug:
                        print "shared cache @ %s" % self._name
                    self._name = fname
                except Exception, err:
                    if _debug:
                        print "open file failed @ %r" % self._name
                    raise err
        else:
            try:
                self._fp = open(self._name, "w+")
                if _debug:
                    print "shared cache @ %s" % self._name
            except Exception, err:
                if _debug:
                    print "open file railed @ %s" % self._name
                raise err

        if(self._sysstr =="Windows"):
            if not self._live:
                try:
                    self._mmapObject = mmap.mmap(self._fp.fileno(), self._size, access = mmap.ACCESS_WRITE, tagname = fname)
                    self._live = 1
                except Exception, err:
                    if _debug:
                        print "map file failed"
                    raise err
        elif(self._sysstr == "Linux"):
            if not self._live:
                try:
                    self._fp.truncate(self._size) 
                    self._mmapObject = mmap.mmap(self._fp.fileno(), self._size, access = mmap.ACCESS_WRITE)
                    self._live = 1     
                except Exception, err:
                    if _debug:
                        print "map file failed"
                    raise err

    def attachObject(self):
        fpath, fname = os.path.split(self._name)
        if not (os.path.exists(fpath) or '' == fpath):
            try:
                os.mkdir(fpath)
                self._fp = open(self._name, "r+")
                if _debug:
                    print "shared cache @%s" % self._name
            except:
                try:
                    self._fp = open(fname, "r+")
                    if _debug:
                        print "shared cache @%s" % self._name
                    self._name = fname
                except Exception, err:
                    if _debug:
                        print "attach failed @ %s" % self._name
                    raise err
        else:
            try:
                self._fp = open(self._name, "r+")
            except Exception, err:
                if _debug:
                    print "attach failed @ %s" % self._name
                raise err

        if(self._sysstr =="Windows"):
            if not self._live:
                try:
                    self._mmapObject = mmap.mmap(self._fp.fileno(), self._size, access = mmap.ACCESS_WRITE, tagname = fname)
                    self._live = 1
                except Exception, err:
                    if _debug:
                        print "map file failed"
                    raise err
        elif(self._sysstr == "Linux"):
            if not self._live:
                try:
                    self._mmapObject = mmap.mmap(self._fp.fileno(), self._size, access = mmap.ACCESS_WRITE)
                    self._live = 1
                except Exception, err:
                    if _debug:
                        print "map file failed"
                    raise err
            
        self._mmapObject.seek(0)	

    def readObject(self, num):
        try:
            objectstr = self._mmapObject.read(num)
            return pickle.loads(objectstr)
        except Exception, err:
            if _debug:
                print "read mmap failed %r" % err
            raise err

    #the protocol value is useing the default value
    def writeObject(self, object):
        try:
            self._mmapObject.write(pickle.dumps(object, pickle.HIGHEST_PROTOCOL))
        except Exception, err:
            if _debug:
                print "write mmap failed %r" % err
            raise err

    def flushObject(self):
        try:
            self._mmapObject.flush()
        except Exception, err:
            if _debug:
                print "flush mmap failed"
            raise err

    #note: enlarge could not take effect here, from the python forum's info it is a python bug
    def __enlargeObject(self, size):
        if size < self._size:
            return
        self._mmapObject.resize(size)
        self._size = size

    def getPosition(self):
        try:
            pos = self._mmapObject.tell()
            return pos
        except Exception, err:
            if _debug:
                print "tell mmap failed"
            raise err

    def setPosition(self, position):
        try:
            self._mmapObject.seek(position)
        except Exception, err:
            if _debug:
                print "seek mmap failed"
            raise err

    #get the shared file size which would be larger than the shared area
    def getWholeSize(self):
        return self._mmapObject.size()

    #attention: this function would damage the fs
    def deleteObject(self):
        try:
            self._mmapObject.close()
            self._fp.close()
            os.remove(self._name)
        except Exception, err:
            if _debug:
                print "fail to delete object %r" % self._fp
            raise err

#NOTE: if need to have the rwlock for the shared cache, the most easy way would use the multiprocessing lib, however, it could only used between the parent and child.
#      ther other two way would use the signal and use the kernel module to control. However use the signal would easily cause deadlock and perf is quit low. use the kernel is best, however the cold would have dependency
#      as a result, do not use any lock at all, treat the misstake as a cache miss
class SharedCache(SharedObject):
    """this dict is used for sharing cache. the perf would be lower than the directly access the memory version"""
    def __init__(self, name, size, mode = ''):
        headDict = {}
        self._pid = os.getpid()
        self._dict = collections.OrderedDict()    #py 2.7 and newer
        self._numEntries = len(self._dict)
        self._sortLevel = 0
        self._head = {}

        if (size > _sharedcachemaxsize):
            size = _sharedcachemaxsize
        elif (size < _sharedcachereserve):
            size = _sharedcachereserve

        super(SharedCache, self).__init__(name, size + _sharedcacheheadsize) #_sharedcacheheadsize is reserved for the control info in the header
        headDict['headfull'] = False
        headDict['proc'] = {}
        headDict['proc'][self._pid] = {}
        headDict['proc'][self._pid]['dirty'] = True #take care for the boundery overlap

        if 'c' == mode:
            self.createObject()
        else:
            self.attachObject()

        try:
            self.setPosition(0)
            self._head = self.readObject(_sharedcacheheadsize)
        except EOFError:
            if(0 == len(self._head)):
                if _debug:
                    print "Try to create the header by %d" % self._pid

                self._head.update(headDict)
                self.setPosition(0)
                self.writeObject(self._head)
                curPos = self.getPosition()

                if(curPos > (_sharedcacheheadsize - _sharedcachegap)):
                    self._head['headfull'] = True
                    self.setPosition(0)
                    self.writeObject(self._head)

                self.flushObject()
                return
            else:
                if _debug:
                    print "***abnormal cache error take more code here!***"
                raise EOFError

        if('headfull' in self._head):
            if(self._head['headfull'] == True):
                if _debug:
                    print "The header is full, no more process could attach to it!"
                raise EOFError

            elif(self._head['headfull'] == False):
                self._head['proc'].update(headDict['proc'])
                self.setPosition(0)
                self.writeObject(self._head)
                self.flushObject()
                curPos = self.getPosition()

                if(curPos > (_sharedcacheheadsize - _sharedcachegap)):
                    self._head['headfull'] = True
                    self.setPosition(0)
                    self.writeObject(self._head)
                    self.flushObject()
        else:
            if _debug:
                print "The header is in mess, could not use!"
            raise KeyError

        self.flushObject()
        self.setPosition(0)


    def __setitem__(self, key, value):
        self.__updateCache()
        self._dict.__setitem__(key, value)
        self._numEntries = len(self._dict)

        self.setPosition(0)
        self._head = self.readObject(_sharedcacheheadsize)

        for k in self._head['proc']:
            if (k != self._pid):
                self._head['proc'][k]['dirty'] = True
        
        self.setPosition(0)
        self.writeObject(self._head)
        self.flushObject()

        try:
            self.setPosition(_sharedcacheheadsize)
            self.writeObject(self._dict)

            curPos = self.getPosition()
            if (curPos > (self._size - _sharedcachegap)):
                popnum = self._numEntries/_sharedcachewatermark
                if popnum < 1:
                    popnum = 1
                for i in range(popnum):
                    self._dict.popitem(False)
                if _debug:
                    print "pop out %d" % popnum

                self.setPosition(_sharedcacheheadsize)
                self.writeObject(self._dict)
                self.flushObject()

        except Exception, err:
            if _debug:
                curPos = self.getPosition()
                if(_sharedcacheheadsize == curPos):
                    print "cache gap size is small since pos is %d" % curPos
                print "writing err %r"
                raise err
    
    def __updateCache(self):
        self.setPosition(0)
        try:
            self._head = self.readObject(_sharedcacheheadsize)
        except EOFError:
            if _debug:
                print "header might be overflow!!!"

        dirty = False
        try:
            for k in self._head['proc']:
                dirty = (dirty or self._head['proc'][k]['dirty'])
        except Exception, err:
            if _debug:
                print "head dict err %r" % err
            raise err

        if (True == dirty):
            try:
                self.setPosition(_sharedcacheheadsize)
                self._dict = self.readObject(self._size)
            except EOFError:
                if _debug:
                    print "first time to load the empty"
            
            self._numEntries = len(self._dict)

            self.setPosition(0)
            self._head['proc'][self._pid]['dirty'] = False
            self.writeObject(self._head)
            self.flushObject()
            if _debug:
                print "clean %d dirty" % self._pid

    def __getitem__(self, key):
        self.__updateCache()
        if(self._numEntries == 0):
            return None
        try:
            val = self._dict.__getitem__(key)
        except:
            return None

        self._dict.pop(key)
        self._dict[key] = val   #move it to the head

        self._sortLevel = self._sortLevel + 1
        if (self._sortLevel > self._numEntries/(_sharedcachewatermark * 2)):
            self.setPosition(_sharedcacheheadsize)
            self.writeObject(self._dict)
            self.flushObject()
            self._sortLevel = 0
            if _debug:
                print "resort cache"

        return val

    def writeThrough():
        pass

    def writeBack():
        pass

#testing code blow
testEntryNum = 10

def procProducer():
    print "producer>>>>>"
    start = datetime.datetime.now()
    created = 0

    f = open("lock", "w+")
    print f.readline()

    while not created:
        try:
            sc = SharedCache(str(0), testEntryNum, 'c')
            created = 1
        except IOError:
            print "creating..."
            time.sleep(1)

    tmpstr = ''
    for index in range(testEntryNum):
        tmpstr = tmpstr + str(index)

    while True:
        for i in range(testEntryNum):
            val = sc[i]
            if val != None: print val

            sc[i] = str(os.getpid())
            time.sleep(1)
    #print sc._dict
    print datetime.datetime.now() - start
    print ">>>>>"

def procConsumer():
    print "consumer>>>>>"
    start = datetime.datetime.now()
    attached = 0

    while not attached:
        try:
            sc = SharedCache(str(0), testEntryNum)
            attached = 1
        except IOError:
            time.sleep(1)
            print "attach..."

    while True:
        for i in range(testEntryNum):
            val = sc[i]
            if val != None: print val

            sc[i] = str(os.getpid())
            time.sleep(1)
    #sc.deleteObject()
    print datetime.datetime.now() - start
    print ">>>>>"

def procProducerDistributed():
    
    print "producer>>>>>"
    start = datetime.datetime.now()

    for i in range(1, testEntryNum, 1):
        sc = SharedCache(str(i), testEntryNum, 'c')
        sc[i] = i
    #print sc._dict
    print datetime.datetime.now() - start
    print ">>>>>"

def procConsumerDistributed():

    print "consumer>>>>>"
    start = datetime.datetime.now()

    for i in range(1, testEntryNum, 1):
        sc = SharedCache(str(i), testEntryNum)
        val = sc[i]
        #print val
        sc.deleteObject()

    print datetime.datetime.now() - start
    print ">>>>>"

if __name__ == "__main__":
    
    start = datetime.datetime.now()
    #prod = multiprocessing.Process(target=procProducer)
    #prod.start()
    procProducer()
    #cons = multiprocessing.Process(target=procConsumer)
    #cons.start()
    #procConsumer()

    #procProducerDistributed()
    #procConsumerDistributed()
    #prod.join()
    #cons.join()
    print "total use: %s" % (datetime.datetime.now() - start)
