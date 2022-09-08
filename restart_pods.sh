kubectl scale deployment worker-deployment --replicas=0
kubectl scale deployment coordinator-deployment --replicas=0
kubectl scale deployment frontend-deployment --replicas=0

sleep 3

kubectl scale deployment worker-deployment --replicas=6
kubectl scale deployment coordinator-deployment --replicas=1
kubectl scale deployment frontend-deployment --replicas=1
