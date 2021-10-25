protos:
	python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. csci4220_hw3.proto
clean:
	rm -rf *pb2*