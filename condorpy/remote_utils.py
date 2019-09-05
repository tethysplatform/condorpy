import os

import paramiko
import scp


class RemoteClient(object):
    def __init__(self,
                 host,
                 username,
                 password=None,
                 private_key=None,
                 private_key_pass=None,
                 port=22):
        self.host = host
        self.username = username
        self.password = password
        if private_key:
            private_key = os.path.expanduser(private_key)
            self.private_key = paramiko.RSAKey.from_private_key_file(filename=private_key, password=private_key_pass)
        self.port = port
        self._transport = None
        self._scp = None
        self._sftp = None

    def __del__(self):
        self.close()

    @property
    def transport(self):
        if self._transport is None or not self._transport.is_active():
            sock = (self.host, self.port)
            self._transport = paramiko.Transport(sock)
            self._transport.connect(username=self.username, password=self.password, pkey=self.private_key)
        return self._transport

    @property
    def sftp(self):
        if self._sftp is None or self._sftp.sock.closed:
            self._sftp = paramiko.SFTPClient.from_transport(self.transport)
        return self._sftp

    @property
    def scp(self):
        if self._scp is None or not self._scp.transport.is_active():
            self._scp = scp.SCPClient(self.transport)
        return self._scp

    def _get_output(self, session):
        stdout = session.makefile('r', -1)
        stdout = '\n'.join([line for line in stdout.readlines()])
        stderr = session.makefile_stderr('r', -1)
        stderr = '\n'.join([line for line in stderr.readlines()])

        return stdout, stderr

    def execute(self, command):
        session = None
        try:
            session = self.transport.open_session()
            session.exec_command(command)
            stdout, stderr = self._get_output(session)
            exit_status = session.recv_exit_status()
            if exit_status != 0:
                msg = "The command '{0}' failed on host '{1}':\n{2}\n{3}".format(command, self.host, stdout, stderr)
                raise RuntimeError(msg)
        finally:
            session and session.close()

        return stdout, stderr

    def remote_file(self, remote_file_path, mode='w'):
        return self.sftp.open(remote_file_path, mode)

    def makedirs(self, remote_path):
        try:
            self.sftp.stat(remote_path)
        except IOError:
            dirname, basename = os.path.split(remote_path.rstrip('/'))
            if dirname:
                self.makedirs(dirname)
            if basename:
                self.sftp.mkdir(remote_path)

    def put(self, local_paths, remote_path):
        self.scp.put(files=local_paths,
                     remote_path=remote_path,
                     recursive=True)

    def get(self, remote_paths, local_path='.'):
        self.scp.get(remote_paths,
                     local_path,
                     recursive=True)

    def close(self):
        if self._sftp is not None:
            self._sftp.close()
        if self._transport is not None:
            self._transport.close()

