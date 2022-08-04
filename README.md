# sfaas-dataflow

To run benchmark:

1. Adjust the `client/workload.ini` file to the desired parameters
2. Start Kafka: `./start-kafka.sh`
3. Start Universalis: `./start-universalis.sh` (you can supply the number of workers as an arg.
   e.g: `./start-universalis 4`)
4. Start Client: `./start-client.sh`

Once the client has finished, the results can be found in `/client/results`