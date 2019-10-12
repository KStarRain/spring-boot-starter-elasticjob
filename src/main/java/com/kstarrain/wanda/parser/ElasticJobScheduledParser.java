package com.kstarrain.wanda.parser;


import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.JobTypeConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.config.script.ScriptJobConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.event.rdb.JobEventRdbConfiguration;
import com.dangdang.ddframe.job.executor.handler.JobProperties.JobPropertiesEnum;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import com.kstarrain.wanda.annotation.ElasticJobScheduled;
import com.kstarrain.wanda.contant.ElasticJobContant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * ElasticJob解析类
 */
@Slf4j
public class ElasticJobScheduledParser implements ApplicationContextAware {
	

	@Autowired
	private ZookeeperRegistryCenter zookeeperRegistryCenter;

	private String prefix_scheduled = "spring.elasticjob.scheduled";

	private String prefix_event_enabled = "spring.elasticjob.event.enabled";
	
	private Environment environment;

	
	public void setApplicationContext(ApplicationContext ctx) throws BeansException {
		environment = ctx.getEnvironment();
		Map<String, Object> beanMap = ctx.getBeansWithAnnotation(ElasticJobScheduled.class);
		for (Object confBean : beanMap.values()) {
			Class<?> clz = confBean.getClass();
			String jobTypeName = confBean.getClass().getInterfaces()[0].getSimpleName();
			ElasticJobScheduled conf = clz.getAnnotation(ElasticJobScheduled.class);

			String jobClass = clz.getName();
			String jobName = conf.name();
			String cron = getEnvironmentStringValue(jobName, ElasticJobContant.CRON, conf.cron());
			String shardingItemParameters = getEnvironmentStringValue(jobName, ElasticJobContant.SHARDING_ITEM_PARAMETERS, conf.shardingItemParameters());
			String description = getEnvironmentStringValue(jobName, ElasticJobContant.DESCRIPTION, conf.description());
			String jobParameter = getEnvironmentStringValue(jobName, ElasticJobContant.JOB_PARAMETER, conf.jobParameter());
			String jobExceptionHandler = getEnvironmentStringValue(jobName, ElasticJobContant.JOB_EXCEPTION_HANDLER, conf.jobExceptionHandler());
			String executorServiceHandler = getEnvironmentStringValue(jobName, ElasticJobContant.EXECUTOR_SERVICE_HANDLER, conf.executorServiceHandler());

			String jobShardingStrategyClass = getEnvironmentStringValue(jobName, ElasticJobContant.JOB_SHARDING_STRATEGY_CLASS, conf.jobShardingStrategyClass());
			String eventDataSourceBeanName = getEnvironmentStringValue(jobName, ElasticJobContant.EVENT_DATA_SOURCE_BEAN_NAME, conf.eventDataSourceBeanName());
			String scriptCommandLine = getEnvironmentStringValue(jobName, ElasticJobContant.SCRIPT_COMMAND_LINE, conf.scriptCommandLine());

			boolean failover = getEnvironmentBooleanValue(jobName, ElasticJobContant.FAILOVER, conf.failover());
			boolean misfire = getEnvironmentBooleanValue(jobName, ElasticJobContant.MISFIRE, conf.misfire());
			boolean overwrite = getEnvironmentBooleanValue(jobName, ElasticJobContant.OVERWRITE, conf.overwrite());
			boolean enabled = getEnvironmentBooleanValue(jobName, ElasticJobContant.ENABLED, conf.enabled());
			boolean monitorExecution = getEnvironmentBooleanValue(jobName, ElasticJobContant.MONITOR_EXECUTION, conf.monitorExecution());
			boolean streamingProcess = getEnvironmentBooleanValue(jobName, ElasticJobContant.STREAMING_PROCESS, conf.streamingProcess());

			int shardingTotalCount = getEnvironmentIntValue(jobName, ElasticJobContant.SHARDING_TOTAL_COUNT, conf.shardingTotalCount());
			int monitorPort = getEnvironmentIntValue(jobName, ElasticJobContant.MONITOR_PORT, conf.monitorPort());
			int maxTimeDiffSeconds = getEnvironmentIntValue(jobName, ElasticJobContant.MAX_TIME_DIFF_SECONDS, conf.maxTimeDiffSeconds());
			int reconcileIntervalMinutes = getEnvironmentIntValue(jobName, ElasticJobContant.RECONCILE_INTERVAL_MINUTES, conf.reconcileIntervalMinutes());

			// 核心配置
			JobCoreConfiguration coreConfig =
					JobCoreConfiguration.newBuilder(jobName, cron, shardingTotalCount)
							.shardingItemParameters(shardingItemParameters)
							.description(description)
							.failover(failover)
							.jobParameter(jobParameter)
							.misfire(misfire)
							.jobProperties(JobPropertiesEnum.JOB_EXCEPTION_HANDLER.getKey(), jobExceptionHandler)
							.jobProperties(JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER.getKey(), executorServiceHandler)
							.build();

			// 不同类型的任务配置处理
			JobTypeConfiguration typeConfig = null;
			if (jobTypeName.equals("SimpleJob")) {
				typeConfig = new SimpleJobConfiguration(coreConfig, jobClass);
			}

			if (jobTypeName.equals("DataflowJob")) {
				typeConfig = new DataflowJobConfiguration(coreConfig, jobClass, streamingProcess);
			}

			if (jobTypeName.equals("ScriptJob")) {
				typeConfig = new ScriptJobConfiguration(coreConfig, scriptCommandLine);
			}

			LiteJobConfiguration jobConfig = LiteJobConfiguration.newBuilder(typeConfig)
					.overwrite(overwrite)
					.disabled(!enabled)
					.monitorPort(monitorPort)
					.monitorExecution(monitorExecution)
					.maxTimeDiffSeconds(maxTimeDiffSeconds)
					.jobShardingStrategyClass(jobShardingStrategyClass)
					.reconcileIntervalMinutes(reconcileIntervalMinutes)
					.build();

			List<BeanDefinition> elasticJobListeners = getTargetElasticJobListeners(conf);

			// 构建SpringJobScheduler对象来初始化任务
			BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(SpringJobScheduler.class);
			factory.setScope(BeanDefinition.SCOPE_PROTOTYPE);
			if ("ScriptJob".equals(jobTypeName)) {
				factory.addConstructorArgValue(null);
			} else {
				factory.addConstructorArgValue(confBean);
			}
			factory.addConstructorArgValue(zookeeperRegistryCenter);
			factory.addConstructorArgValue(jobConfig);



			// 任务执行事件记录的数据源，以Bean名称获取
            boolean eventEnabled = getEventEnabledFromEnvironment();
			if (eventEnabled && StringUtils.hasText(eventDataSourceBeanName)) {
				BeanDefinitionBuilder rdbFactory = BeanDefinitionBuilder.rootBeanDefinition(JobEventRdbConfiguration.class);
				rdbFactory.addConstructorArgReference(eventDataSourceBeanName);
				factory.addConstructorArgValue(rdbFactory.getBeanDefinition());
			}

			factory.addConstructorArgValue(elasticJobListeners);
			DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory)ctx.getAutowireCapableBeanFactory();
			String springJobSchedulerBeanName = StringUtils.uncapitalize(clz.getSimpleName()) + "_SpringJobScheduler";
			defaultListableBeanFactory.registerBeanDefinition(springJobSchedulerBeanName, factory.getBeanDefinition());
			SpringJobScheduler springJobScheduler = (SpringJobScheduler) ctx.getBean(springJobSchedulerBeanName);
			springJobScheduler.init();
			log.info("【" + jobName + "】\t" + jobClass + "\tinit success");
		}


	}


    private List<BeanDefinition> getTargetElasticJobListeners(ElasticJobScheduled conf) {
		List<BeanDefinition> result = new ManagedList<BeanDefinition>(2);
		String listeners = getEnvironmentStringValue(conf.name(), ElasticJobContant.LISTENER, conf.listener());
		if (StringUtils.hasText(listeners)) {
			BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(listeners);
			factory.setScope(BeanDefinition.SCOPE_PROTOTYPE);
			result.add(factory.getBeanDefinition());
		}

		String distributedListeners = getEnvironmentStringValue(conf.name(), ElasticJobContant.DISTRIBUTED_LISTENER, conf.distributedListener());
		long startedTimeoutMilliseconds = getEnvironmentLongValue(conf.name(), ElasticJobContant.DISTRIBUTED_LISTENER_STARTED_TIMEOUT_MILLISECONDS, conf.startedTimeoutMilliseconds());
		long completedTimeoutMilliseconds = getEnvironmentLongValue(conf.name(), ElasticJobContant.DISTRIBUTED_LISTENER_COMPLETED_TIMEOUT_MILLISECONDS, conf.completedTimeoutMilliseconds());

		if (StringUtils.hasText(distributedListeners)) {
			BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(distributedListeners);
			factory.setScope(BeanDefinition.SCOPE_PROTOTYPE);
			factory.addConstructorArgValue(startedTimeoutMilliseconds);
			factory.addConstructorArgValue(completedTimeoutMilliseconds);
			result.add(factory.getBeanDefinition());
		}
		return result;
	}

	/**
	 * 获取配置中的任务属性值，environment没有就用注解中的值
	 * @param jobName		任务名称
	 * @param fieldName		属性名称
	 * @param defaultValue  默认值
	 * @return
	 */
	private String getEnvironmentStringValue(String jobName, String fieldName, String defaultValue) {
		String key = prefix_scheduled + jobName + "." + fieldName;
		String value = environment.getProperty(key);
		if (StringUtils.hasText(value)) {
			return value;
		}
		return defaultValue;
	}

	private int getEnvironmentIntValue(String jobName, String fieldName, int defaultValue) {
		String key = prefix_scheduled + jobName + "." + fieldName;
		String value = environment.getProperty(key);
		if (StringUtils.hasText(value)) {
			return Integer.valueOf(value);
		}
		return defaultValue;
	}

	private long getEnvironmentLongValue(String jobName, String fieldName, long defaultValue) {
		String key = prefix_scheduled + jobName + "." + fieldName;
		String value = environment.getProperty(key);
		if (StringUtils.hasText(value)) {
			return Long.valueOf(value);
		}
		return defaultValue;
	}

	private boolean getEnvironmentBooleanValue(String jobName, String fieldName, boolean defaultValue) {
		String key = prefix_scheduled + jobName + "." + fieldName;
		String value = environment.getProperty(key);
		if (StringUtils.hasText(value)) {
			return Boolean.valueOf(value);
		}
		return defaultValue;
	}

    private boolean getEventEnabledFromEnvironment() {
        String value = environment.getProperty(prefix_event_enabled);
        if (StringUtils.hasText(value)) {
            return Boolean.valueOf(value);
        }
        return false;
    }
}
