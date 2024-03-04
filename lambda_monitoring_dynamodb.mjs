import { CloudWatchClient, GetMetricStatisticsCommand } from "@aws-sdk/client-cloudwatch";
import { DynamoDBClient, DescribeTableCommand, DescribeTableReplicaAutoScalingCommand, UpdateTableCommand } from "@aws-sdk/client-dynamodb";
import { ApplicationAutoScalingClient, DescribeScalableTargetsCommand, RegisterScalableTargetCommand } from "@aws-sdk/client-application-auto-scaling"; // ES Modules import


const REGION = "us-east-1";
const TABLE_NAME = 'AWSCookbook406';
const END_DATE = new Date();
const INTERVAL = 10;
const PERIOD = 1;
const RANGE = 4;
const START_DATE = new Date(END_DATE);
START_DATE.setMinutes(END_DATE.getMinutes() - INTERVAL) ;
const CONSUMED_READ_CAPACITY_METRIC = 'ConsumedReadCapacityUnits';
const CONSUMED_WRITE_CAPACITY_METRIC = 'ConsumedWriteCapacityUnits';
const PROVISIONED_READ_CAPACITY_METRIC = 'ProvisionedReadCapacityUnits';
const PROVISIONED_WRITE_CAPACITY_METRIC = 'ProvisionedWriteCapacityUnits';

export const handler = async (event) => {
  
  console.log("===================CONST VALUES====================");
  console.log("Region: ", REGION);
  console.log("Table name: ", TABLE_NAME);
  console.log("End date: ", END_DATE);
  console.log("Interval: ", INTERVAL);
  console.log("Start date: ", START_DATE);
  console.log("===================================================");
  
  const data = await getCloudWatchMetrics();
  console.log(data.responseReadCap.Datapoints);
  console.log(data.responseWriteCap.Datapoints);
  console.log(data.responseProvReadCap.Datapoints);
  console.log(data.responseProvWriteCap.Datapoints);
  
  const capacities = await describeProvisionedUnits();
  
  console.log(capacities);

  const readCp = compareCapacity(data.responseReadCap.Datapoints, capacities.read.min, capacities.read.max);
  
  const writeCp = compareCapacity(data.responseWriteCap.Datapoints, capacities.write.min, capacities.write.max);
  
  console.log(readCp);
  console.log(writeCp);
  
  if(readCp.condition !== false || writeCp.condition !== false) 
    await updateCapacity(readCp.value, writeCp.value);

  // TODO implement
  const response = {
    statusCode: 200,
    body: JSON.stringify('Hello from Lambda!'),
  };
  return response;
};


async function getCloudWatchMetrics(){
  const cw = new CloudWatchClient({ apiVersion: "2010-08-01" });
  
  const metricReadCap = customCWParams(CONSUMED_READ_CAPACITY_METRIC);
  const metricWriteCap = customCWParams(CONSUMED_WRITE_CAPACITY_METRIC);
  const metricProvReadCap = customCWParams(PROVISIONED_READ_CAPACITY_METRIC);
  const metricProvWriteCap = customCWParams(PROVISIONED_WRITE_CAPACITY_METRIC);
  
  const commandReadCap = new GetMetricStatisticsCommand(metricReadCap);
  const commandWriteCap = new GetMetricStatisticsCommand(metricWriteCap);
  const commandProvReadCap = new GetMetricStatisticsCommand(metricWriteCap);
  const commandProvWriteCap = new GetMetricStatisticsCommand(metricWriteCap);
  const responseReadCap = await cw.send(commandReadCap);
  const responseWriteCap = await cw.send(commandWriteCap);
  const responseProvReadCap = await cw.send(commandProvReadCap);
  const responseProvWriteCap = await cw.send(commandProvWriteCap);
  
  return {
    responseReadCap,
    responseWriteCap,
    responseProvReadCap,
    responseProvWriteCap
  }
}

function customCWParams (metricName){
  return {
    EndTime: END_DATE, 
    MetricName: metricName, 
    Namespace: 'AWS/DynamoDB', 
    Period: PERIOD, 
    StartTime: START_DATE,
    Dimensions: [{'Name': 'TableName', 'Value': TABLE_NAME}],
    Statistics: [
      "Sum"
    ],
    Unit: "Count"
  }
}

async function describeProvisionedUnits(){
  const clientAAS = new ApplicationAutoScalingClient();
  const inputAASR = { // DescribeScalingPoliciesRequest
    "ServiceNamespace": "dynamodb" , // required
    "ResourceIds": [`table/${TABLE_NAME}`],
    "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
  };
  
  const inputAASW = { // DescribeScalingPoliciesRequest
    "ServiceNamespace": "dynamodb" , // required
    "ResourceIds": [`table/${TABLE_NAME}`],
    "ScalableDimension": "dynamodb:table:WriteCapacityUnits",
  };

  const commandAASR = new DescribeScalableTargetsCommand(inputAASR);
  const responseAASR = await clientAAS.send(commandAASR);
  
  const commandAASW = new DescribeScalableTargetsCommand(inputAASW);
  const responseAASW = await clientAAS.send(commandAASW);

  return {
    read: {
      min: responseAASR.ScalableTargets[0].MinCapacity,
      max: responseAASR.ScalableTargets[0].MaxCapacity,

    },
    write: {
      min: responseAASW.ScalableTargets[0].MinCapacity,
      max: responseAASW.ScalableTargets[0].MaxCapacity,
    }
  };
}

function compareCapacity(collection, min, max){
  collection.sort((a,b)=> b.Sum - a.Sum);  
  const value = collection[0].Sum;
  const defaultUnit = 2; 

  if(value > min && value > max){
     console.log("===============INCREASE MAX=================");
    return { value: Math.ceil(value), condition: true };
  } else if (value < min && value < max) {
    console.log("================DECREASE=====================");
    console.log(value);
    const _value = (value === 0) ? defaultUnit : value;
    return {value: Math.ceil(_value), condition: true };
  } else if(value >= min && value <= max){
    console.log("==================REMAIN=====================");
    return {value: value, condition: false };
  } else {
    return;
  }
}

async function updateCapacity(read, write){
  const inputRead = {
    "ServiceNamespace": "dynamodb",
    "ResourceId": `table/${TABLE_NAME}`,
    "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
    "MinCapacity": read,
    "MaxCapacity": read + RANGE,
  };
  
   const inputWrite = {
    "ServiceNamespace": "dynamodb",
    "ResourceId": `table/${TABLE_NAME}`,
    "ScalableDimension": "dynamodb:table:WriteCapacityUnits",
    "MinCapacity": write,
    "MaxCapacity": write + RANGE,
  };
  
  const client = new ApplicationAutoScalingClient();
  const commandRead = new RegisterScalableTargetCommand(inputRead);
  const responseRead = await client.send(commandRead);
  console.log("================================================");
  console.log(responseRead);
 
  const commandWrite = new RegisterScalableTargetCommand(inputWrite);
  const responseWrite = await client.send(commandWrite);
  console.log("================================================");
  console.log(responseWrite);
}
