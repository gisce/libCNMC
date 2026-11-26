from mamba import description, it


with description('Public package imports'):
    with it('imports the model packages'):
        import libcnmc.models
        import libcnmc.models.res_4667

    with it('imports the circular packages'):
        import libcnmc.cir_3_2015
        import libcnmc.cir_4_2014
        import libcnmc.cir_4_2015
        import libcnmc.cir_8_2021

    with it('imports the resolution and audit packages'):
        import libcnmc.audit_6181
        import libcnmc.res_4131
        import libcnmc.res_4603
        import libcnmc.res_4666
        import libcnmc.res_4667
        import libcnmc.res_4771
