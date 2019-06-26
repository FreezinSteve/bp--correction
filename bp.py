
""" neonrest contains functions to access NEON data using RESTful web services.

    Functions available are:
        neoneRestRequest
        getSession
        getNodeList
        getChannelList
        getData
"""

import requests
# from io import StringIO
import pandas as pd
import getpass

# Private functions and variables used internally within the module
_DateFormat = '%Y-%m-%dT%H:%M:%S'


def _neonRestRequest(Host, Page, Payload={}, Headers={}):
    RequestURL = Host + '/' + Page
    Response = requests.get(RequestURL, Payload, headers=Headers)
    return Response.json()


def _dataDictSeries2PdSeries(DataDict):
    global _DateFormat
    SamplesSeries = pd.DataFrame(DataDict['Samples'])
    SamplesSeries.Time = pd.to_datetime(SamplesSeries.Time,
                                        format=_DateFormat)
    SamplesSeries.set_index('Time', inplace=True)
    SamplesSeries = SamplesSeries.Value.astype(float)
    DataDict['Samples'] = SamplesSeries
    return DataDict


# Main functions
def getSession(Host, Username=None, Password=None):
    """ Get an authorisation token for accessing the aquarius server.

        Host = the location of the aquarius server
        Username, Password = Optional, If not supplied the user will be
                             prompted to input them.

        e.g. AuthToken = getAuthToken('aquarius.niwa.co.nz', 'MyUsername',
                                      'MyPassword')
    """
    if Username is None:
        Username = input('Aquarius username: ')
    if Password is None:
        Password = getpass.getpass('Aquarius password: ')
    Payload = {'u': Username, 'p': Password}
    Response = _neonRestRequest(Host, 'GetSession', Payload)
    AuthToken = {'X-Authentication-Token': Response['Token']}
    return AuthToken


def getNodeList(Host, AuthToken):
    """ Get a list of available nodes.

        Host = the location of the aquarius server
    """
    Response = _neonRestRequest(Host, 'GetNodeList', Headers=AuthToken)
    NodeList = pd.DataFrame.from_dict(Response['GetNodeListResult'])
    return NodeList


def getChannelList(Host, AuthToken, NodeId, ShowInactive=False):
    """ Get a list of available channels for a specified node.

        Host = the location of the aquarius server
    """
    Payload = {'ShowInactive': ShowInactive}
    Response = _neonRestRequest(Host, 'GetChannelList/' + str(NodeId),
                                Payload=Payload, Headers=AuthToken)
    ChannelList = pd.DataFrame.from_dict(Response['GetChannelListResult'])
    return ChannelList


def getData(Host, AuthToken, ChannelId, StartTime, EndTime, DstAdjust=False):
    """ Get data for a single specified channel.

        Host = the location of the aquarius server
    """
    global _DateFormat
    Payload = {'StartTime': StartTime.strftime(_DateFormat),
               'EndTime': EndTime.strftime(_DateFormat),
               'DSTAdjust': DstAdjust}
    Response = _neonRestRequest(Host, 'GetData/' + str(ChannelId),
                                Payload=Payload, Headers=AuthToken)
    Data = _dataDictSeries2PdSeries(Response['GetDataResult'])
    return Data


def getDataMultiChannel(Host, AuthToken, ChannelIdList,
                        StartTime, EndTime, DstAdjust=False):
    """ Get data for a multiple channels.

        Host = the location of the aquarius server
    """
    global _DateFormat
    Payload = {'Channels': ','.join(map(str, ChannelIdList)),
               'StartTime': StartTime.strftime(_DateFormat),
               'EndTime': EndTime.strftime(_DateFormat),
               'DSTAdjust': DstAdjust}
    Response = _neonRestRequest(Host, 'GetDataMultiChannel',
                                Payload=Payload, Headers=AuthToken)
    DataMultiChannel = Response['GetDataMultiChannelResult']
    for Data in DataMultiChannel:
        Data = _dataDictSeries2PdSeries(Data)
    return DataMultiChannel


# 1 hPa = 10.1971636 mmH2O = 0.0102 m of water

host = 'http://restservice-neon1.niwa.co.nz/NeonRESTService.svc'
# Get the BP from the Larundal CWS node iD 3401
token = getSession(host,'xxxxx','xxxxx')
channels = getChannelList(host,token,3401)
bp=0
for item in channels.iterrows():
    if item[1]['Name'] == 'Barometric Pressure(AVG)':
        bp = float(item[1]['LastValue'])
        break

if bp > 0:
    # Convert to m of water
    bp = bp - 1013          # standard pressure
    depth = bp * 0.0102     # convert to m of water
    # Save to file for web pickup
    f = open("/var/www/html/bpcorr.txt", "w")
    f.write(str(round(depth,2)))
    f.write('!')
    f.close()

