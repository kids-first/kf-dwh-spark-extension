#!groovy
properties([
    pipelineTriggers([[$class:"SCMTrigger", scmpoll_spec:"* * * * *"]])
])

pipeline {
  agent { label 'terraform-testing' }

  stages {
    stage('Get Code'){
      steps {
        deleteDir()
        checkout scm
      }
    }
    stage("Create ssh_config file") {
      steps {
        sh '''
        mkdir config/
        echo "Host *" > config/sshd_config
        echo "SendEnv LANG LC_*\n HashKnownHosts yes \n GSSAPIAuthentication yes \n GSSAPIDelegateCredentials no \n" >> config/sshd_config
        '''
      }
    }
    stage('Build JAR'){
      steps{
        pending("${env.JOB_NAME}","prd")
        sh '''
           ./build.sh
          '''
        success("${env.JOB_NAME}","prd")
      }
      post {
        failure {
          fail("${env.JOB_NAME}","prd")
        }
      }
    }
    stage('Finished Building') {
      steps{
        success("${env.JOB_NAME}","prd")
      }
    }
  }
}

void success(projectName,syslevel) {
//   sendStatusToGitHub(projectName,"success")
  slackSend (color: '#00FF00', message: "${projectName}:smile: Deployed to ${syslevel}: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
}

void fail(projectName,syslevel) {
//   sendStatusToGitHub(projectName,"failure")
  slackSend (color: '#ff0000', message: "${projectName}:frowning: Deployed to ${syslevel} Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
}

void pending(projectName, syslevel) {
  //sendStatusToGitHub(projectName, "pending")
  slackSend (color: '#FFFF00', message: "${projectName}:sweat_smile:Starting to deploy to ${syslevel}: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
}

void sendStatusToGitHub(projectName,status) {
sh """
export VAULT_ADDR='https://vault-dev.kids-first.io'
vault auth -method=aws role=aws-infra-jenkins_devops
"""
env.GITHUB_TOKEN = sh(script: "export VAULT_ADDR='https://vault-dev.kids-first.io' && set +x &&  vault read -field=value /secret/devops/jenkins-kf-github-token && set -x", returnStdout: true)
sh """
 set +x
 curl 'https://api.github.com/repos/kids-first/kf-lib-shiro-ego-realm/statuses/$GIT_COMMIT?access_token=$GITHUB_TOKEN' -H 'Content-Type: application/json' -X POST -d '{\"state\": \"${status}\", \"description\": \"Jenkins\", \"target_url\": \"$BUILD_URL\"}'
 set -x
"""
}
