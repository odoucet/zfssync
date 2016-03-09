<?php
date_default_timezone_set('UTC');

$errors = "";

$parameters = parse_ini_file(__DIR__.'/parameters.ini');
if (!$parameters) {
    throw new Exception('Cannot read parameters.ini');
}

if (!function_exists('ssh2_connect')) {
    echo "This script requires SSH2 extension\n";
    die(-1);
}

try {
    // Connect to distant host
    $distantSsh = new NiceSSH();
    $distantSsh->connect($parameters);

    // Interactive session
    $distantInteractiveSsh = new NiceSSH();
    $distantInteractiveSsh->connect($parameters);

    printf(
        "%s Retrieving distant volumes ...\n",
        date('Y-m-d H:i:s')
    );
    // distant list of volumes
    $distantVolumes = explode("\n", trim($distantSsh->exec('/sbin/zfs list -H -o name -rt filesystem,volume '.$parameters['zfs_distant_path'])));

    printf(
        "%s Retrieving local volumes ...\n",
        date('Y-m-d H:i:s')
    );
    // Local list of volumes
    $localVolumes = explode("\n", trim(shell_exec('/sbin/zfs list -H -o name -rt filesystem,volume '.$parameters['zfs_local_path'])));
    $volumesToCreate = array();

    $yesterday = date('Y-m-d', time()-86400).'.000000Z';
    $today     = date('Y-m-d').'.000000Z';
    
    // Diff !
    foreach ($localVolumes as $i => $line) {
        if (trim($line) == '') {
            continue;
        }
        $newName = str_replace($parameters['zfs_local_path'], $parameters['zfs_distant_path'], $line);
        if (!in_array($newName, $distantVolumes)) {
            $found = false;
            foreach (explode(',', $parameters['zfs_ignore_paths']) as $ignore) {
                if ($ignore == substr($line, 0, strlen($ignore))) {
                    $found = true;
                    unset($localVolumes[$i]);
                    break;
                }
            }

            if ($found === false) {
                $volumesToCreate[] = $line;
            }
        }
    }

    printf(
        "%s We have %d existing volumes that do not exist on target.\n",
        date('Y-m-d H:i:s'),
        count($volumesToCreate)
    );

    if (count($volumesToCreate) > 0) {
        sort($volumesToCreate);
        $i = 0;
        foreach ($volumesToCreate as $vol) {
            $start = microtime(true);
            $distantName = str_replace($parameters['zfs_local_path'], $parameters['zfs_distant_path'], $vol);

            printf(
                "%s [%4.1f%%] Syncing  %-60s ...\n",
                date('Y-m-d H:i:s'),
                $i/count($localVolumes)*100,
                $vol
            );

            echo $distantInteractiveSsh->shell("/usr/bin/mbuffer -q -s 128k -W 600 -m 100M -4 -I 31330|/sbin/zfs recv -F '".$distantName."' 2>&1 & ".PHP_EOL);
            // need to wait a little for process to spawn
            sleep(1);

            $str =  "/sbin/zfs send  '".$vol."@".$today."' | ".
                    "/usr/bin/mbuffer -q -s 128k -W 600 -m 100M -O '".$parameters['ssh_host'].":31330' 2>&1";

            $returnString = shell_exec($str);
            wait_for_local_mbuffer();
            wait_for_distant_mbuffer($distantSsh);
            if ($returnString == '') {
                    printf(
                        "%s [%4.1f%%] Synced  %-60s OK in %7.2f seconds\n",
                        date('Y-m-d H:i:s'),
                        $i/count($volumesToCreate)*100,
                        $vol,
                        microtime(true)-$start
                    );

            } else {
                printf(
                    "%s [%4.1f%%] Synced  %-60s FAILED: %s\n",
                    date('Y-m-d H:i:s'),
                    $i/count($localVolumes)*100,
                    $vol,
                    $returnString
                );
                kill_local_mbuffer();
            }
            $i++;
        }
    }

    // Diff
    printf(
        "%s We have %d volumes to sync with incremental method.\n",
        date('Y-m-d H:i:s'),
        count($localVolumes)
    );

    $i = 0;
    foreach ($localVolumes as $vol) {
        $distantName = str_replace($parameters['zfs_local_path'], $parameters['zfs_distant_path'], $vol);

        // find if distant destination already exists
        $hasSnap = trim($distantSsh->exec('zfs list -H -o name '.$distantName.'@'.$today));
        if ($hasSnap == $distantName.'@'.$today) {
            //Yes, already have this one !
            printf("%s [%4.1f%%] %-60s already synced !\n", date('Y-m-d H:i:s'), $i/count($localVolumes)*100, $vol);

        } else {
            // find if distant src exists
            $hasOrigin = trim($distantSsh->exec('/sbin/zfs list -H -o name '.$distantName.'@'.$yesterday));
            if ($hasOrigin == $distantName.'@'.$yesterday) {
                $srcSnapshot = $yesterday;
            } else {
                $srcSnapshot = null;
                for ($j = 1; $j<10; $j++) {
                    $testSnap = date('Y-m-d', time()-86400*$j);
                    $hasOrigin = trim($distantSsh->exec(
                        '/sbin/zfs list -H -o name '.$distantName.'@'.$testSnap.'.000000Z'
                    ));
                    if ($hasOrigin == $distantName.'@'.$testSnap.'.000000Z') {
                        $srcSnapshot = date('Y-m-d', time()-86400*$j).'.000000Z';
                        break;
                    }
                }
            }

            if ($srcSnapshot === null) {
                printf(
                    "%s [%4.1f%%] ERROR WITH %-60s: cannot find source snapshot suitable.\n",
                    date('Y-m-d H:i:s'),
                    $i/count($localVolumes)*100,
                    $vol
                );

            } else {
                printf("%s [%4.1f%%] Syncing %s from @%s ...", date('Y-m-d H:i:s'), $i/count($localVolumes)*100, $vol, $srcSnapshot);
                $start = microtime(true);

                echo $distantInteractiveSsh->shell("/usr/bin/mbuffer -q -s 128k -W 600 -m 100M -4 -I 31330|/sbin/zfs recv -F '".$distantName."' 2>&1 & ".PHP_EOL);
                // need to wait a little for process to spawn
                usleep(100000);

                $str = "/sbin/zfs send -I '".$vol."@".$srcSnapshot."' '".$vol."@".$today."' | ".
                       "/usr/bin/mbuffer -q -s 128k -W 600 -m 100M -O '".$parameters['ssh_host'].":31330' 2>&1";
                $returnString = shell_exec($str);

                // ZFS take time to finish and free parent mbuffer process, so we just wait a little.
                $z = wait_for_local_mbuffer();
                $y = wait_for_distant_mbuffer($distantSsh);
                if ($z === false || $y === false) {
                    echo "\nCannot stop wait_*_mbuffer(), exiting\n";
                    die(-1);
                }

                if ($returnString == '') {
                    printf("OK in %7.2f seconds\n", microtime(true)-$start);

                } else {
                    printf("FAILED: %s\n", $returnString);
                    kill_local_mbuffer();
                }
            }
        }
        
        $i++;
    }

    // Cleanup
    $pid = trim($distantSsh->exec("ps fauxw |fgrep mbuffer |grep ' 31330'|grep -v grep |awk '{print $2}'"));
    if ($pid != '') {
        echo "Cleaning mbuffer process #".$pid."\n";
        echo $distantInteractiveSsh->exec('kill '.$pid);
    }

    kill_local_mbuffer();

    $distantSsh->disconnect();
    $distantInteractiveSsh->disconnect();

    printf("%s Finished !\n", date('Y-m-d H:i:s'));

} catch (Exception $e) {
    echo "An error occured\n";
    echo $e->getMessage();
    echo "----------\n";
    var_dump($e);
    debug_print_backtrace();
}


function kill_local_mbuffer()
{
    $pid = trim(shell_exec("ps fauxw |fgrep mbuffer |grep ' 31330'|grep -v grep |awk '{print $2}'"));
    if ($pid != '') {
        echo "Cleaning local mbuffer process #".$pid."\n";
        echo shell_exec('kill '.$pid);
    }
}

function wait_for_local_mbuffer()
{
    for ($i = 0; $i<=6000; $i++) {
        $pid = trim(shell_exec("ps fauxw |fgrep mbuffer |grep ' 31330' |grep -v grep |awk '{print $2}'"));
        if ($pid != '') {
            echo ".";
            usleep(50000);
        } else {
            break;
        }
    }
    if ($i == 6000) {
        echo "Wait too much, sorry\n";
        return false;
    }
    //echo "\n";
    return true;
}

function wait_for_distant_mbuffer($connection)
{
    for ($i = 0; $i<=6000; $i++) {
        $pid = trim($connection->exec("ps fauxw |fgrep mbuffer |grep ' 31330' |grep -v grep |awk '{print $2}'".PHP_EOL));
        if ($pid != '') {
            echo "+";
            usleep(50000);
        } else {
            break;
        }
    }
    if ($i == 6000) {
        echo "Wait too much, sorry\n";
        return false;
    }
    //echo "\n";
    return true;
}


class NiceSSH
{
    // SSH Connection
    private $connection;

    private $parameters;

    private $methods = array(
        'client_to_server' => array(
            'crypt' => 'arcfour',
        ),
        'server_to_client' => array(
            'crypt' => 'arcfour',
        )
    );

    public $shell;
   
    public function connect($parameters)
    {
        // Load parameters
        $this->parameters = $parameters;

        if (!($this->connection = ssh2_connect(
            $this->parameters['ssh_host'],
            $this->parameters['ssh_port'],
            $this->methods
        ))) {
            throw new Exception('Cannot connect to server');
        }

        $fingerprint = strtolower(ssh2_fingerprint($this->connection, SSH2_FINGERPRINT_MD5 | SSH2_FINGERPRINT_HEX));
        if (
            strtolower(preg_replace('@[^a-f0-9]{1,}@i', '', $this->parameters['ssh_fingerprint'])) !=
            $fingerprint) {
            throw new Exception('Unable to verify server identity! Server returned this fingerprint: '.$fingerprint);
        }

        if (!ssh2_auth_pubkey_file(
            $this->connection,
            $this->parameters['ssh_auth_user'],
            $this->parameters['ssh_auth_pub'],
            $this->parameters['ssh_auth_priv'],
            $this->parameters['ssh_auth_pass']
        )) {
            throw new Exception('Autentication rejected by server');
        }
    }

    public function shell($cmd)
    {
        if ($this->shell === null) {
            $this->shell = ssh2_shell($this->connection, 'xterm');
        }

        if (!($stream = fwrite($this->shell, $cmd))) {
            throw new Exception('SSH shell command failed');
        }
    }

    public function exec($cmd)
    {
        if (!($stream = ssh2_exec($this->connection, $cmd))) {
            throw new Exception('SSH command "'.$cmd.'"" failed');
        }
        stream_set_blocking($stream, true);
        $data = "";
        while ($buf = fread($stream, 4096)) {
            $data .= $buf;
        }
        fclose($stream);
        return $data;
    }

    public function disconnect()
    {
        if (is_resource($this->connection)) {
            if ($this->shell === null) {
                ssh2_exec($this->connection, 'logout');
            } else {
                fwrite($this->shell, 'logout'.PHP_EOL);
            }
            $this->connection = null;
        }
    }

    public function __destruct()
    {
        $this->disconnect();
    }
}

