import math
import sys
from datetime import datetime

MICROS_PER_SECOND = 1_000_000

class Graph(object):
    """
    Creating the graph from the message received
    """
    def __init__(self):
        self.adjacencyList = {}

    def AddEdge(self, node1, node2, weight):
        if(node1 in self.adjacencyList):
            self.adjacencyList[node1][node2] = weight
        else:
            self.adjacencyList[node1] = {node2 : weight}

    def RemoveEdge(self, node1, node2):
        """
        utility funtion for removing the stale edges
        """
        if(node1 in self.adjacencyList):
            self.adjacencyList[node1].pop(node2, None)

    def GetNodeCount(self):
        return len(self.adjacencyList)

    def GetNodes(self):
        return self.adjacencyList.keys()
    
    def GetEdgesForNode(self, node):
        return self.adjacencyList[node]

    def GetEdgesWeight(self, node1, node2):
        if(node1 in self.adjacencyList):
            if(node2 in self.adjacencyList[node1]):
                return self.adjacencyList[node1][node2]
        return None

class BellmanFord(object):
    """
    Executing the bellman ford algorithm to detect the negative cycle
    """
    def __init__(self, g):
        self.distanceEstimate = {}
        self.parentGraph = {}
        self.graph = g
        self.tolerance = float(1e-12)

        for node in self.graph.GetNodes():
            self.distanceEstimate[node] = math.inf
            self.parentGraph[node] = None
        self.distanceEstimate['USD'] = 0

    def RelaxEdge(self, node1, node2, weight):
        """
        relaxing the edges to get the minimum weight
        :param node1: currency1 in the published message
        :param node2: currency2 in the published message
        :param weight: exchange rate received in the publisher message for currency1 to currency2
        """
        newEstimate = self.distanceEstimate[node1] + weight
        if self.distanceEstimate[node2] > newEstimate:
            improvement = self.distanceEstimate[node2] - newEstimate
            if improvement > self.tolerance:
                self.distanceEstimate[node2] = newEstimate
                self.parentGraph[node2] = node1

    def ComputeNegativeCycleNodes(self, node):
        """
        Computing the Arbitrage
        :param node: currency2 of the published message lists's first dictionary
        """
        temp = node
        visited = set()
        backTrackedNodes = []
        negativeCycleNodes = []

        while True:
            visited.add(temp)
            backTrackedNodes.insert(0, temp)
            temp = self.parentGraph[temp]
            if temp is None:
                print("Unexpected parent found while printing the cycle.")
                return None
            if temp in visited:
                backTrackedNodes.insert(0, temp)
                break
        
        prevNode = backTrackedNodes.pop(0)
        weightSum = 0
        for node in backTrackedNodes:
            negativeCycleNodes.insert(0, node)
            weightSum += self.graph.GetEdgesWeight(prevNode, node)
            prevNode = node
            if node == temp:
                break

        if weightSum > -1 * self.tolerance:
            print(f"\nIgnoring negative cycle as sum of weights in cycle '{weightSum}' is greater than -{self.tolerance}.")
            return []

        return negativeCycleNodes

    def DetectNegativeCycle(self):
        """
        Checking for any arbitrage opportunity
        """
        for __ in range(1, self.graph.GetNodeCount() - 1):
            for node1 in self.graph.GetNodes():
                for node2, weight in self.graph.GetEdgesForNode(node1).items():
                    self.RelaxEdge(node1, node2, weight)
        
        for node1 in self.graph.GetNodes():
            for node2, weight in self.graph.GetEdgesForNode(node1).items():
                if self.distanceEstimate[node2] > self.distanceEstimate[node1] + weight:
                    return self.ComputeNegativeCycleNodes(node2)
        
        return None

class ArbitrageDetector(object):
    """
    Check the graph for the negative cycles
    """
    def __init__(self):
        self.graph = Graph()
        self.lastPriceUpdate = {}
        self.exchangeRate = {}
        self.priceExpirationInterval = 1.5 * MICROS_PER_SECOND

    def GetCurrentTimeInMicroSeconds(self):
        epoch = datetime(1970, 1, 1)
        microSeconds = (datetime.utcnow() - epoch).total_seconds() * MICROS_PER_SECOND
        return microSeconds

    def ProcessPublishedPrice(self, timeStamp, currency1, currency2, exchangeRate):
        """
        Using the nodes, price and time from the published message
        and updating the edges of the graph
        :param timestamp: timestamp received in the published message
        :param currency1 & currency2: currencies received in the published message
        :param exchangeRate: exchange rate of the currencies received in the published message 
        """
        logWeight = math.log(exchangeRate)
        self.graph.AddEdge(currency1, currency2, -1 * logWeight)
        self.graph.AddEdge(currency2, currency1, logWeight)

        self.lastPriceUpdate[(currency1, currency2)] = timeStamp

        self.exchangeRate[(currency1, currency2)] = exchangeRate
        self.exchangeRate[(currency2, currency1)] = 1/exchangeRate

    def RemoveExpiredEdges(self):
        """
        Removing the staloe edges from the graph
        """
        currentTime = self.GetCurrentTimeInMicroSeconds()
        quotesToRemove = set()
        for currenyTuple, lastUpdated in self.lastPriceUpdate.items():
            if(currentTime - lastUpdated >= self.priceExpirationInterval):
                print(f"\nRemoving stale quote for {currenyTuple}")
                quotesToRemove.add(currenyTuple)
                currency1, currency2 = currenyTuple
                self.graph.RemoveEdge(currency1, currency2)
                self.graph.RemoveEdge(currency2, currency1)

        for currenyTuple in quotesToRemove:
            self.lastPriceUpdate.pop(currenyTuple)

    def PrintArbitrage(self, negativeCycleNodes):
        """
        :param negativeCycleNodes: list of nodes in the negative cycle
        #Rotate list to being USD to front and append to end
        """
        if 'USD' not in negativeCycleNodes:
            print("\nUSD is not part of negative cycle nodes.")
            return
        
        negativeCycleNodes.reverse()
        lastIdx = len(negativeCycleNodes) - 1
        while negativeCycleNodes[lastIdx] != 'USD':
            lastItem = negativeCycleNodes.pop(lastIdx)
            negativeCycleNodes.insert(0, lastItem)
        
        print("\nARBITRAGE:")
        print("\t start with USD 100")
        prevNode = 'USD'
        prevVal = 100
        for currNode in negativeCycleNodes:
            exchangeRate = self.exchangeRate[(prevNode, currNode)]
            currVal = prevVal * exchangeRate
            print(f"\t exchange {prevNode} for {currNode} at {exchangeRate} --> {currNode} {currVal}")
            prevNode = currNode
            prevVal = currVal

    def CheckForArbitrage(self):
        """
        starting the bellman ford to check the arbitrage
        """
        self.RemoveExpiredEdges()
        bellmanFord = BellmanFord(self.graph)
        negativeCycleNodes = bellmanFord.DetectNegativeCycle()
        if negativeCycleNodes is not None and len(negativeCycleNodes) > 0:
            self.PrintArbitrage(negativeCycleNodes)
