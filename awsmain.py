import boto3
from threading import Thread
from botocore.exceptions import WaiterError

access_key=""
secret_key =""
global ec2
global resource
global ssm
def init_aws():
    #AWS SDK module init
    global ec2
    global resource
    global ssm

    ec2 = boto3.client('ec2',aws_access_key_id=access_key,aws_secret_access_key=secret_key,region_name="ap-northeast-2")
    ssm = boto3.client('ssm',aws_access_key_id=access_key,aws_secret_access_key=secret_key,region_name="ap-northeast-2")
    resource = boto3.resource('ec2',aws_access_key_id=access_key,aws_secret_access_key=secret_key,region_name="ap-northeast-2")
    

#인스턴스 리스트 출력
def listInstances():
    print("Listing instances...")
    done = False
    while done==False:
        list=resource.instances.all()
        for instance in list:
            print("[id] %s, [AMI] %s, [type] %s, [state] %10s, [monitoring state] %s" % (instance.instance_id,instance.image_id,instance.instance_type,instance.state['Name'],instance.monitoring['State']))

        done=True

#Zone 출력
def availableZones():
    print("Available zones...")
    done = False
    while done==False:
        res=ec2.describe_availability_zones()
        list=res['AvailabilityZones']
        for zone in list:
            print("[id] %s  [region] %15s [zone] %15s" % (zone['ZoneId'], zone['RegionName'], zone['ZoneName']))

        done=True

#인스턴스 시작
def startInstance(id):
    print("Starting .... %s" % id)
    done= False
    while done==False:
        res=ec2.start_instances(InstanceIds=[id])
        instance=resource.Instance(id)
        #인스턴스 시작 시 까지 대기
        instance.wait_until_running(Filters=[{'Name': 'instance-id','Values':[id]}])
        print("Successfully started instance %s" % id)
        done=True

#가용 리전 출력
def availableRegions():
    print("Available regions...")
    done = False
    while done==False:
        res=ec2.describe_regions()
        list=res['Regions']
        for region in list:
            print("[region] %15s, [endpoint] %s" % (region['RegionName'],region['Endpoint']))

        done=True

#인스턴스 정지
def stopInstance(id):
    print("Stopping .... %s" % id)
    done= False
    while done==False:
        res=ec2.stop_instances(InstanceIds=[id])
        instance=resource.Instance(id)
        #인스턴스 종료 시 까지 대기
        instance.wait_until_stopped(Filters=[{'Name': 'instance-id','Values':[id]}])
        print("Successfully stopped instance %s" % id)
        done=True

#인스턴스 생성
def createInstance(id):
     print("Creating...")
     done= False
     while done==False:
        #htcondor-slave-image 기반 생성, 보안그룹 설정
        res=resource.create_instances(ImageId=id,InstanceType='t2.micro',MaxCount=1,MinCount=1,SecurityGroupIds=['sg-038024dd64c7f9fa5'])
        instance=resource.Instance(res[0].instance_id)
        #인스턴스 실행 시 까지 대기
        instance.wait_until_running(Filters=[{'Name': 'instance-id','Values':[res[0].instance_id]}])
        print("Successfully started EC2 instance %s based on AMI %s" % (res[0].instance_id,id))
        done=True

#인스턴스 재시작
def rebootInstance(id):
    print("Rebooting .... %s" % id)
    done= False
    while done==False:
        res=ec2.reboot_instances(InstanceIds=[id])
        instance=resource.Instance(id)
        #인스턴스 실행 시 까지 대기
        instance.wait_until_running(Filters=[{'Name': 'instance-id','Values':[id]}])
        print("Successfully rebooted instance %s" % id)
        done=True

#이미지 리스트 출력
def listImages():
    print("Listing images ....")
    done = False
    while done==False:
        #이름이 htcondor-slave-image인 이미지만 출력
        res=ec2.describe_images(Filters=[{'Name':'name','Values':['htcondor-slave-image']}])
        list=res['Images']
        for image in list:
            print("[ImageID] %s, [Name] %s, [Owner] %s" % (image['ImageId'],image['Name'],image['OwnerId']))

        done=True

#condor status 확인
def condor_status():
    #ssm module로 통신
    res=ssm.send_command(InstanceIds=['i-0fa386b594da20771'],DocumentName='AWS-RunShellScript',Parameters={'commands': ['condor_status']})
    command_id = res['Command']['CommandId']

    #커맨드 실행 결과 받을때 까지 대기
    waiter = ssm.get_waiter("command_executed")
    try:
        waiter.wait(
        CommandId=command_id,
        InstanceId='i-0fa386b594da20771',
        )
    except WaiterError as ex:
        logging.error(ex)
        return

    #커맨드 실행 결과 출력
    print(ssm.get_command_invocation(CommandId=command_id, InstanceId='i-0fa386b594da20771')['StandardOutputContent']) 

#condor_q 상태 확인
def condor_q(): 
    #ssm module로 통신
    res=ssm.send_command(InstanceIds=['i-0fa386b594da20771'],DocumentName='AWS-RunShellScript',Parameters={'commands': ['condor_q']})
    command_id = res['Command']['CommandId']

    #커맨드 실행 결과 받을때 까지 대기
    waiter = ssm.get_waiter("command_executed")
    try:
        waiter.wait(
        CommandId=command_id,
        InstanceId='i-0fa386b594da20771',
        )
    except WaiterError as ex:
        logging.error(ex)
        return

    #커맨드 실행 결과 출력
    print(ssm.get_command_invocation(CommandId=command_id, InstanceId='i-0fa386b594da20771')['StandardOutputContent']) 


#오토스케일링 모듈(multithreading)
def autoscaling():
    while True:
        #ssm을 통해 master의 shell script 실행
        # queue의 job 개수, slot 개수, job이 배치된 instance private-ip 가져오기
        res=ssm.send_command(InstanceIds=['i-0fa386b594da20771'],DocumentName='AWS-RunShellScript',Parameters={'commands': [". /home/ec2-user/autoscaling.sh"]})
        command_id = res['Command']['CommandId']
        #커맨드 실행 대기
        waiter = ssm.get_waiter("command_executed")
        try:
            waiter.wait(
            CommandId=command_id,
            InstanceId='i-0fa386b594da20771',
            )
        except WaiterError as ex:
            logging.error(ex)
            return

        #결과 가져오기
        result=ssm.get_command_invocation(CommandId=command_id, InstanceId='i-0fa386b594da20771')['StandardOutputContent']    
        slot=result[0] #슬롯 갯수
        jobs=result[1] #큐 작업 갯수
        sched=result[2] # 작업이 배치된 노드 private ip
        sched=sched.split('\n') # private ip string list split

        if slot<jobs: #슬롯보다 큐 작업이 많으면
            print('scale out')
            createInstance('ami-0825246f21ab11a8c') #slave image 기반 인스턴스 생성

        elif slot>jobs: #슬롯이 큐 작업보다 많으면
            print('scale in')

            #실행 중인 인스턴스 목록 가져오기
            res=list(resource.instances.filter(Filters=[{'Name':'instance-state-name', 'Values':['running']}]))
            result=[]

            #작업 배치된 슬롯이 아니면 append
            for i in res:
                for j in range(0,len(sched)-1):
                    if i.private_ip_address != sched[j]:
                        result.append(i)

            #모든 노드가 작업 배치된 것이 아니면
            if len(result)!= 0:
                #리스트 맨 마지막 인스턴스
                id=result[-1].instance_id
                #master 노드가 아니면
                if id !='i-0fa386b594da20771':
                    stopInstance(id) #인스턴스 종료
            else:
                pass

        # 큐 작업 == 슬롯 갯수
        else:
            print('stable')     

def scaling(): #무한 루프 제외 autoscaling 모듈과 동일 로직
    res=ssm.send_command(InstanceIds=['i-0fa386b594da20771'],DocumentName='AWS-RunShellScript',Parameters={'commands': [". /home/ec2-user/autoscaling.sh"]})
    command_id = res['Command']['CommandId']
    waiter = ssm.get_waiter("command_executed")
    try:
        waiter.wait(
        CommandId=command_id,
        InstanceId='i-0fa386b594da20771',
        )
    except WaiterError as ex:
        logging.error(ex)
        return
    result=ssm.get_command_invocation(CommandId=command_id, InstanceId='i-0fa386b594da20771')['StandardOutputContent']    
    slot=result[0]
    jobs=result[1]
    sched=result[2:]
    sched=sched.split('\n')
    if slot<jobs:
        print('scale out')
        createInstance('ami-0825246f21ab11a8c')
    elif slot>jobs:
        print('scale in')
        res=list(resource.instances.filter(Filters=[{'Name':'instance-state-name', 'Values':['running']}]))
        result=[]

        for i in res:
            for j in range(0,len(sched)-1):
                if i.private_ip_address != sched[j]:
                    result.append(i)

        if len(result)!= 0:
            id=result[-1].instance_id
            if id !='i-0fa386b594da20771':
                stopInstance(id)
        else:
            pass
    else:
        print('stable')     
 

if __name__ == "__main__":
    init_aws()
    #Thread(target=autoscaling).start() #autoscaling을 위한 multithread 
    while True:
        print("------------------------------------------------------------")
        print("           Amazon AWS Control Panel using SDK               ")
        print("------------------------------------------------------------")
        print("  1. list instance                2. available zones        ")
        print("  3. start instance               4. available regions      ")
        print("  5. stop instance                6. create instance        ")
        print("  7. reboot instance              8. list images            ")
        print("  9. condor_status               10. scaling                ")
        print("  11. condor_q                   99. exit                   ")
        print("------------------------------------------------------------")

        print("Enter an integer: ",end="")
        num=int(input())

        if num == 1:
            listInstances()
        
        elif num == 2:
            availableZones()
        
        elif num == 3:
            print("Enter instance id: ",end="")
            id=input()
            startInstance(id)
        
        elif num == 4:
            availableRegions()
        
        elif num == 5:
             print("Enter instance id: ",end="")
             id=input()
             stopInstance(id)
        
        elif num == 6:
            print("Enter ami id: ",end="")
            id=input()
            createInstance(id)
        
        elif num == 7:
             print("Enter instance id: ",end="")
             id=input()
             rebootInstance(id)
        
        elif num == 8:
            listImages()

        elif num == 9:
            condor_status()
        
        elif num == 10:
            scaling()
        
        elif num == 11:
            condor_q()

        elif num == 99:
            print("bye!")
            exit(0)
        else:
            print("concertration")