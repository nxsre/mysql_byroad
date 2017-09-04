-- phpMyAdmin SQL Dump
-- version 4.1.9
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: 2016-08-05 15:56:47
-- 服务器版本： 5.6.30-1+deb.sury.org~xenial+2-log
-- PHP Version: 5.6.24-1+deb.sury.org~xenial+1


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `byroad`
--
CREATE DATABASE IF NOT EXISTS `byroad` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
USE `byroad`;

-- --------------------------------------------------------

--
-- 表的结构 `notify_field`
--

CREATE TABLE IF NOT EXISTS `notify_field` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `schema` varchar(120) NOT NULL,
  `table` varchar(120) NOT NULL,
  `column` varchar(120) NOT NULL,
  `send` int(11) NOT NULL,
  `task_id` int(11) NOT NULL,
  `create_time` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- 表的结构 `task`
--

CREATE TABLE IF NOT EXISTS `task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(120) NOT NULL,
  `apiurl` varchar(255) NOT NULL,
  `event` varchar(120) NOT NULL,
  `stat` varchar(120) NOT NULL,
  `create_time` datetime NOT NULL,
  `create_user` varchar(120) NOT NULL,
  `routine_count` int(11) NOT NULL,
  `re_routine_count` int(11) NOT NULL,
  `re_send_time` int(11) NOT NULL,
  `retry_count` int(11) NOT NULL,
  `timeout` int(11) NOT NULL,
  `desc` varchar(255) DEFAULT NULL,
  `pack_protocal` int(11) DEFAULT NULL,
  `db_instance_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- 表的结构 `tasklog`
--

CREATE TABLE IF NOT EXISTS `tasklog` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` int(11) NOT NULL,
  `message` varchar(1000) DEFAULT NULL,
  `reason` varchar(1000) DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

ALTER TABLE `byroad`.`task` 
ADD COLUMN `phone_numbers` VARCHAR(255) NOT NULL AFTER `db_instance_name`,
ADD COLUMN `emails` VARCHAR(255) NOT NULL AFTER `phone_numbers`,
ADD COLUMN `alert` INT NOT NULL AFTER `emails`;

CREATE TABLE IF NOT EXISTS `config` (
  `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
  `key` varchar(120) NOT NULL,
  `value` varchar(120) NOT NULL,
  `description` varchar(120)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

ALTER TABLE `byroad`.`task` 
ADD COLUMN `audit_state` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '任务审计状态' AFTER `alert`,
ADD COLUMN `push_state` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '任务推送状态' AFTER `audit_state`,
ADD COLUMN `update_time` DATETIME NOT NULL DEFAULT NOW() COMMENT '任务更新时间' AFTER `push_state`,
ADD COLUMN `category` VARCHAR(120) NOT NULL DEFAULT '' COMMENT '任务分组' AFTER `update_time` ;

ALTER TABLE `byroad`.`notify_field` 
ADD COLUMN `audit_state` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '审计状态' AFTER `create_time`,
ADD COLUMN `audit_id` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '审计数据id' AFTER `audit_state`,
ADD COLUMN `update_time` DATETIME DEFAULT NOW() COMMENT '字段更新时间' AFTER `audit_id`;

CREATE TABLE `audit` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `apply_user` varchar(255) NOT NULL DEFAULT '' COMMENT '申请人',
  `audit_user` varchar(255) NOT NULL DEFAULT '' COMMENT '审计人',
  `apply_type` int(11) NOT NULL DEFAULT 0 COMMENT '申请类型（新建或更新等）',
  `state` int(11) NOT NULL DEFAULT 0 COMMENT '审计状态',
  `task_id` int(11) NOT NULL DEFAULT 0 COMMENT '审计的任务id',
  `create_time` datetime NOT NULL DEFAULT NOW() COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT NOW() COMMENT '更新时间',
  PRIMARY KEY (`id`)
) COMMENT='任务审计表' ENGINE=InnoDB  DEFAULT CHARSET=utf8;

CREATE TABLE `user` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `username` varchar(255) NOT NULL DEFAULT '' COMMENT 'auth用户名',
  `fullname` varchar(255) NOT NULL DEFAULT '' COMMENT '用户全名',
  `role` int(11) NOT NULL DEFAULT '0' COMMENT '用户角色',
  `permissions` varchar(255) NOT NULL DEFAULT '' COMMENT '用户权限',
  `mail` varchar(255) NOT NULL DEFAULT '' COMMENT '用户邮箱',
  `create_time` datetime NOT NULL DEFAULT NOW() COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT NOW() COMMENT '更新时间',
  PRIMARY KEY (`id`)
) COMMENT='用户表' ENGINE=InnoDB  DEFAULT CHARSET=utf8;