<?php

/**
 * create database specific phpunit.xml config file for travis.
 * e.g. sqlite.phpunit.xml.
 *
 * @author  cryptocompress <cryptocompress@googlemail.com>
 */
$source = __DIR__.'/../phpunit.xml.dist';
$config = [
    'mysql' => [
        'phpcr.doctrine.dbal.driver' => 'pdo_mysql',
        'phpcr.doctrine.dbal.host' => '127.0.0.1',
        'phpcr.doctrine.dbal.username' => 'root',
        'phpcr.doctrine.dbal.password' => 'root',
        'phpcr.doctrine.dbal.dbname' => 'phpcr_tests',
    ],
    'pgsql' => [
        'phpcr.doctrine.dbal.driver' => 'pdo_pgsql',
        'phpcr.doctrine.dbal.host' => 'localhost',
        'phpcr.doctrine.dbal.username' => 'postgres',
        'phpcr.doctrine.dbal.password' => 'postgres',
        'phpcr.doctrine.dbal.dbname' => 'phpcr_tests',
    ],
    'sqlite' => [
        'phpcr.doctrine.dbal.driver' => 'pdo_sqlite',
        'phpcr.doctrine.dbal.path' => 'phpcr_tests.db',
    ],
];

if (!in_array(@$argv[1], array_keys($config))) {
    exit('Error:'."\n\t".'Database "'.@$argv[1].'" not supported.'."\n".
        'Usage:'."\n\t".'php tests/'.basename(__FILE__).' ['.implode('|', array_keys($config)).']'."\n");
}

$dom = new DOMDocument('1.0', 'UTF-8');
$dom->preserveWhiteSpace = false;
$dom->formatOutput = true;
$dom->strictErrorChecking = true;
$dom->validateOnParse = true;
$dom->load($source);

$xpath = new DOMXPath($dom);
$parent = $xpath->query('/phpunit/php')->item(0);
$nodes = $xpath->query('/phpunit/php/var[starts-with(@name,"phpcr.doctrine.dbal.")]');

foreach ($nodes as $node) {
    $parent->removeChild($node);
}

foreach ($config[$argv[1]] as $key => $value) {
    $node = $dom->createElement('var');
    $node->setAttribute('name', $key);
    $node->setAttribute('value', $value);
    $parent->appendChild($node);
}

$destination = str_replace('phpunit.xml.dist', $argv[1].'.phpunit.xml', $source);
$dom->save($destination);

echo 'Created:'."\n\t".realpath($destination)."\n";
