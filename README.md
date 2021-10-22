# genBehavior

    封装与收集各种有用的erlang行为
    最初目的是想造个非常统一又通用的行为模式-基于这个想法-封装了gen_ipc行为模块
    基于gen_ipc gen_srv 基于Otp24.1.2编写 运行otp版本23.0+

# 简写备注

    gen_ipc         gen_information_processing_cell   
    gen_srv         gen_server
    gen_epm         gen_event_processing module
    gen_emm         gen_event_management module
    gen_tcm         gen_tcp_callback_module
    gen_apu         gen_Automatic_processing_unit

# gen_ipc

    erlang中存在application, supervisor, gen_server, gen_event, gen_fsm, gen_statem各种行为, 学习otp也都会学习这些行为， 实际项目中也经常会用这些行为，
    其中gen_server, gen_event, gen_fsm, gen_statem这些worker类型的行为gen_server用的最多。 从大的方面看很多这些worker类型的行为都属于c/s模型，但是就单从服务进程来看
    响应请求和返回请求只是服务进程对外提供的服务接口，而处理请求才是核心，而服务进程处理各种请求和其他事物可以看做是图灵机， 而gen_statem就是一个完备的图灵机模型，
    基于这样的分析，gen_server, gen_event, gen_fasm可以看做是gen_statem的子集，是简化了的gen_statem, 所以我觉得基于gen_statem把gen_statem, gen_server, gen_event, gen_fsm
    封装成一个统一又通用的行为是可行的--这就得到了gen_ipc行为模块。然而实际工作中会有不一样的场景和需求，而之前gen_server, gen_event, gen_statem就是应用于这些不同的场景和需求，那么在gen_ipc
    中需要兼容这些场景和需求，所以gen_ipc封装的接口函数以及内部封装就提供了这些兼容。如果你想应用gen_server功能， 就可以只用相应的功能， 而且可以做到像gen_server那样快速响应，而不用额外处理像gen_statem
    中各种事件，定时器，各种actions以及状态的中间处理。总结起来就是gen_ipc不仅仅可以单独作为gen_server, gen_event, gen_statem使用， 同时保证了单独使用时的快速响应，还可以作为这些行为的融合使用。
    
    把这些行为封装成一个， 不仅仅可以减少学习成本，最开始学习erlang otp时要学习各种行为，各种用法，稍微有点费神， 还可以在工作中保持基础行为的统一， 不然项目中又是gen_server, 又是gen_event,又
    是gen_statem不是很简洁和易用， 而且有些还不经常用，容易搞错。现在统一了， 天天用一个，天天就看一个就没简单明了多了，而且简单的需求和复杂的场景都满足， 是不是感觉好多了。

# gen_srv

    出于考虑大部分的行为模式为gen_server, 如果使用gen_ipc大量handle函数会添加一个额外无用的 _ 作为 状态 参数的占位， 可能对于一些追求完美与代码简洁的人来说不可接受，
    所以出于该考虑， 基于gen_server结合gen_ipc一些便捷式的定时器封装了gen_srv. 

# gen_emm 
    gen_event 重写

# gen_apu 
    功能与gen_srv一致 只是 call和cast消息 不在调用 handle_call handle_cast函数 而是取消息的第一个参数作为函数名 调用回调模块的这个函数

    
    
    
