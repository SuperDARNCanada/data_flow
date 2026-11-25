from os.path import expanduser
import subprocess as sp

HOME = expanduser("~")
BL_GIT_REPO_DIR = f"{HOME}/mirror_blocklists/DDWG/"


def new_bl_file_available():
    """Checks if a new blocklist file is uploaded via git.
    Returns:
        TYPE: True, if new git update is available.
    """

    # This command will return the number of new commits available in main.
    # This signals that there is a new blocklist file available.
    cmd = f"cd {BL_GIT_REPO_DIR}; git fetch; git log ..origin/main --oneline | wc -l"
    shell_output = sp.check_output(cmd, shell=True)

    return bool(int(shell_output))


def pull_new_bl_file():
    """Uses git to grab the new blocklist updates.
    """
    cmd = f"cd {BL_GIT_REPO_DIR}; git pull origin main"
    sp.call(cmd, shell=True)


if __name__ == '__main__':
    # Check to see if a new file is there
    repo_updated = new_bl_file_available()
    if repo_updated:
        try:
            pull_new_bl_file()
            print('Success ... Git repo updated')
        except:
            print('Update unsuccessful')
    else:
        print('Git repo already up to date, no action taken')
