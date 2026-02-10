# cmake/utils.cmake

# 设置项目范围内的编译选项和属性
function(set_project_properties target)
    target_compile_features(${target} PUBLIC cxx_std_17)

    # 在所有平台上都设置这些警告
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target} PRIVATE
                -Wall
                -Wextra
                -Wpedantic
                -Werror          # 将警告视为错误
                -Wno-unused-parameter
        )
    elseif(MSVC)
        target_compile_options(${target} PRIVATE
                /W4              # 最高的警告等级
                /WX              # 将警告视为错误
        )
    endif()

endfunction()