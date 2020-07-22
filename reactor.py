from reactors.utils import Reactor, agaveutils
import copy
import sys
import json


def submit(r,system,path):
    # Create agave client from reactor object
    ag = r.client
    # copy our job.json from config.yml
    job_def = copy.copy(r.settings.fastqc)
    inputs = job_def["inputs"]
    # Define the input for the job as the file that
    # was sent in the notificaton message
    inputs["fastq"] = "agave://"+system+"/"+path
    job_def.inputs = inputs
    job_def.archiveSystem = system
    # Get the directory, excluding file name, from the path
    dir=path.split('/')[:-1]
    dir='/'.join(dir)
    # Set the job to archive to the 'analyzed' directory
    # in the same folder as the input
    job_def.archivePath = dir + '/analyzed/'

    # Submit the job in a try/except block
    try:
        # Submit the job and get the job ID
        job_id = ag.jobs.submit(body=job_def)['id']
        print(job_id)
        print(json.dumps(job_def, indent=4))
    except Exception as e:
        print(json.dumps(job_def, indent=4))
        print("Error submitting job: {}".format(e))
        print(e.response.content)
        return
    return


def main():
    """Main function"""
    # create the reactor object
    r = Reactor()
    r.logger.info("Hello this is actor {}".format(r.uid))
    # pull in reactor context
    context = r.context
    #print(context)
    # get the file name from the message that was sent to the actor
    file = context.message_dict.file
    #print(file)
    # Get system and path from file message
    system = file.get('systemId')
    path = file.get('path')
    print(system)
    print(path)
    # We don't want to do anything when files are uploaded to the
    # "analyzed" directory
    if path.split('/')[-1] == 'analyzed':
        print("not reacting to the analyzed directory upload")
        sys.exit(0)
    # If the status is "STAGING_COMPLETED", then we'll go ahead and
    # submit the job
    elif context.message_dict.file.status == 'STAGING_COMPLETED':
        submit(r,system,path)
    # Otherwise we'll exit
    else:
        print("Can submit job for status: ", context.message_dict.file.status)
        sys.exit(0)



if __name__ == '__main__':
    main()
