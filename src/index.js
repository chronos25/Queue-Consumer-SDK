require('dotenv').config();
require( 'console-stamp' )( console );

const { Consumer } = require('sqs-consumer');
const timeDelay = 5000;

function createConsumer(handleMessage, sqsQueueUrl){
  try{
    console.log("Received queue url ", sqsQueueUrl);
    const configuration = {
        region: process.env.SQS_REGION,
        endpoint: sqsQueueUrl,
    };

    const app = Consumer.create({
      queueUrl: configuration.endpoint,
      region: configuration.region,
      handleMessage: handleMessage
    }); 

    if(process.env.ENVIRONMENT==="development"){g
      console.log("Development Mode ", process.env.ENVIRONMENT)
        if (
            process.env.SQS_ACCESS_KEY_ID &&
            process.env.SQS_ACCESS_KEY_ID != "" &&
            process.env.SQS_ACCESS_SECRET &&
            process.env.SQS_ACCESS_SECRET != ""
        ) {
            configuration["credentials"] = {
            accessKeyId: process.env.SQS_ACCESS_KEY_ID,
            secretAccessKey: process.env.SQS_ACCESS_SECRET,
            };
        }
        sqsClient = new SQSClient({
            region: configuration.region,
            endpoint: configuration.endpoint,
            credentials: {
            accessKeyId: configuration?.credentials?.accessKeyId,
            secretAccessKey: configuration?.credentials?.secretAccessKey
            }
        });

        app = Consumer.create({
          queueUrl: configuration.endpoint,
          region: configuration.region,
          sqs : sqsClient,
          handleMessage: handleMessage
        });
    }
    
    app.on('error', (data) => {
      try{
        console.log({message:"Will reconnect to SQS...", data});
        setTimeout(()=>{
          console.log({message:"Reconnecting SQS...", data});
          startConsumer();
        },timeDelay);
      } catch(e){
        console.error("Error in sqs consumer.", e);
        process.exit(1);
      }
    });
    
    app.on('processing_error', (data) => {
      try{
        console.log({message:"Will reconnect to SQS...", data});
        setTimeout(()=>{
          console.log({message:"Reconnecting SQS...", data});
          startConsumer();
        },timeDelay);
      } catch(e){
        console.error("Processing error in sqs consumer.");
        process.exit(1);
      }
    });

    app.on('stopped', (data)=>{
      try{
        console.log({message:"Stopping SQS...", data});
      } catch(e){
        console.error("Processing error in sqs consumer.");
        process.exit(1);
      }
    });

    function stopConsumer(){
      console.log({message:"Stopping Consumer"});
      app.stop();
    }
    
    function startConsumer(){
      console.log({message:"Started Consumer"});
      app.start();
    }

    return {
      startConsumer,
      stopConsumer
    }
  } catch(e){
    console.error("Error when consuming messages from queue", e);
    process.exit(1);
  }
}

module.exports = {
    createConsumer
}
