# Automating StreamingAnalytics Provisioning

This PowerShell script will help export and import existing Azure Streaming Analytics job. The idea is that once you have designed the job, you can export it with this script into a JSON definition so that you easily can recreate it again.
Whether you are doing demos, quickly need to create your jobs for testing or just automating your provisioning, this script will save you some time.

## The script functionality
The powershell script can do mutliple things, which are

- Export - export an existing job into a JSON file
- Import - import and recreate a job from a JSON definition file
- Delete - delete the existing job (removes it entierly from Azure - the job, not the datasources)
- Start  - start a Streaming Analytics job
- Stop   - stop a Streaming Analytics job
- Status - check the status of an existing job

## Exporting

Exporting creates two JSON files. One which is the complete job definition and a second that just contains the datasource configuration.  
<pre>
<code>
  ./deploy-StreamingAnalitics.ps1 -Operation export -JobName "my-job-name" 
</code>
</pre>
<img src="http://www.redbaronofazure.com/wp-content/uploads/2016/05/StreamA-1B-export.png"/>

The datasources config file is the only file you need to change when you import the job in another environment.  
<img src="http://www.redbaronofazure.com/wp-content/uploads/2016/05/StreamA-1C-export.png"/>

## Importing

Importing reads the JSON definition file applies the values in the JSON datasources file before recreating the Streaming Analytics Job.

<img src="http://www.redbaronofazure.com/wp-content/uploads/2016/05/StreamA-8A-run-script.png"/>

## Service Bus

The script needs the DLL Microsoft.ServiceBus.dll for configuring keys for the Service Bus. You can get this DLL in multiple ways via NuGet and Visual Studio. I've also included it here in my github repo just for your convenience.

## Testing this script

To use this script you first need to design a Streaming Analytics Job and then export it. After you have exported it you can delete it and then recreate it.

See my blog post for more details about how the script works