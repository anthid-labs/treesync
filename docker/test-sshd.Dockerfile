# A throwaway SSH host for exercising a remote sync.
#
# Not part of the shipped image and not a base for one: it exists so
# `remote-test.sh` has something to sync into that is a genuinely separate
# machine as far as treesync is concerned: its own filesystem, its own user,
# reached over a real SSH connection.
FROM alpine:3.23

RUN apk add --no-cache openssh-server

# Host keys are generated at build time. They are throwaway by construction:
# the image is rebuilt per run and the client trusts them through a
# known_hosts file it writes itself.
RUN ssh-keygen -A

# Key auth only. There is no password on this account to guess, and treesync
# connects with BatchMode=yes, which cannot answer a prompt anyway.
RUN adduser -D -s /bin/sh deploy \
    && passwd -u deploy \
    && mkdir -p /home/deploy/.ssh \
    && chmod 700 /home/deploy/.ssh \
    && chown -R deploy:deploy /home/deploy/.ssh

RUN printf '%s\n' \
    'PermitRootLogin no' \
    'PasswordAuthentication no' \
    'PubkeyAuthentication yes' \
    'AuthorizedKeysFile .ssh/authorized_keys' \
    > /etc/ssh/sshd_config.d/treesync-test.conf

EXPOSE 22

# -D keeps it in the foreground, -e logs to stderr so `docker logs` shows an
# auth failure instead of it disappearing into the container's syslog.
CMD ["/usr/sbin/sshd", "-D", "-e"]
